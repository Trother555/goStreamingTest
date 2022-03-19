package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	pb "streaming/docs"

	"google.golang.org/grpc"
)

type docsServer struct {
	pb.UnimplementedDocsServer
}

func logStreamEnded(cnt int64, secondElapsed float64) {
	fmt.Printf("Finish processing. Reading %d items took %f time\n", cnt, secondElapsed)
}

func logStreamError(cnt int64, secondElapsed float64, err error) {
	fmt.Printf("Stream ended with an error. Read %d items took %f time, err: %s\n", cnt+1, secondElapsed, err)
}

func writeResToFile(result *pb.DocStreamRequest) {
	b, err := json.Marshal(result)
	if err != nil {
		fmt.Printf("And error occured while writing the result: %s\n", err)
	}
	fmt.Printf("Gonna write %d bytes to res file\n", len(b))
	os.WriteFile("result.json", b, 066)
}

var cntFull int64 = 0
var timeFull float64 = 0.0

func (*docsServer) SendDocFull(stream pb.Docs_SendDocFullServer) error {
	fmt.Printf("Start processing SendDocFull\n")
	startTime := time.Now()
	var cnt int64
	var result *pb.DocStreamRequest
	procError := func(err error) (bool, error) {
		if err == io.EOF {
			endTime := time.Now()
			cntFull += 1
			time := endTime.Sub(startTime).Seconds()
			timeFull += time
			fmt.Printf("avg SendDocFull request time: %f\n", timeFull/float64(cntFull))
			logStreamEnded(cnt, time)
			writeResToFile(result)
			return true, stream.SendAndClose(&pb.SendDocResponse{TotarRowsCount: cnt})
		}
		if err != nil {
			endTime := time.Now()
			logStreamError(cnt, endTime.Sub(startTime).Seconds(), err)
			return true, err
		}
		return false, nil
	}
	// receive the first message
	doc, err := stream.Recv()
	done, err := procError(err)
	if err != nil || done {
		return err
	}
	result = doc
	for {
		doc, err := stream.Recv()
		done, err := procError(err)
		if err != nil || done {
			return err
		}
		cnt++
		if len(doc.Rows) > 0 {
			result.Rows = append(result.Rows, doc.Rows...)
		}
	}
}

var cntVariadic int64 = 0
var timeVariadic float64 = 0.0

func (*docsServer) SendDocVariadic(stream pb.Docs_SendDocVariadicServer) error {
	fmt.Printf("Start processing SendDocVariadic\n")
	startTime := time.Now()
	var cnt int64
	var result *pb.DocStreamRequest
	procError := func(err error) (bool, error) {
		if err == io.EOF {
			endTime := time.Now()
			cntVariadic += 1
			time := endTime.Sub(startTime).Seconds()
			timeVariadic += time
			fmt.Printf("avg SendDocVariadic request time: %f\n", timeVariadic/float64(cntVariadic))
			fmt.Printf("total time: %f total cnt: %d\n", timeVariadic, cntVariadic)
			logStreamEnded(cnt, time)
			writeResToFile(result)
			return true, stream.SendAndClose(&pb.SendDocResponse{TotarRowsCount: cnt})
		}
		if err != nil {
			endTime := time.Now()
			logStreamError(cnt, endTime.Sub(startTime).Seconds(), err)
			return true, err
		}
		return false, nil
	}
	doc, err := stream.Recv()
	done, err := procError(err)
	if err != nil || done {
		return err
	}
	switch d := doc.Msg.(type) {
	case *pb.DocOrRowRequest_Doc:
		result = d.Doc
	default:
		fmt.Printf("doc expected not row")
		return nil
	}
	for {
		row, err := stream.Recv()
		done, err := procError(err)
		if err != nil || done {
			return err
		}
		cnt++
		switch r := row.Msg.(type) {
		case *pb.DocOrRowRequest_Row:
			result.Rows = append(result.Rows, r.Row)
		default:
			fmt.Printf("row expected, got: %T", r)
			return nil
		}
	}
}

func newServer() *docsServer {
	s := &docsServer{}
	return s
}

func main() {
	address := fmt.Sprintf("localhost:%d", 8084)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("failed to listen: %v\n", err)
	}
	fmt.Printf("Listening %s\n", address)
	grpcServer := grpc.NewServer()
	pb.RegisterDocsServer(grpcServer, newServer())
	grpcServer.Serve(lis)
}
