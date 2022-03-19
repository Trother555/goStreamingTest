package main

import (
	"context"
	"fmt"
	pb "streaming/docs"
	"time"

	"google.golang.org/grpc"
)

func generateRows() []*pb.Row {
	cnt := 50000
	res := []*pb.Row{}
	for i := 0; i < cnt; i++ {
		res = append(res, &pb.Row{
			Name:    fmt.Sprintf("NameNameNameNameNameNameNameNameNameNameNameNameNameNameName_%d", i),
			Sku:     int64(i),
			Char:    fmt.Sprintf("CharCharCharCharCharCharCharCharCharCharCharCharCharCharCharChar_%d", i),
			Quality: int64(i),
			AddName: fmt.Sprintf("AddNamAddNamAddNamAddNamAddNamAddNamAddNamAddNamAddNamAddNamAddNamAddNamAddNamAddNamAddName_%d", i),
			AddCode: fmt.Sprintf("AddCodeAddCodeAddCodeAddCodeAddCodeAddCodeAddCodeAddCodeAddCodeAddCodeAddCodeAddCode_%d", i),
			Count:   float64(i),
			Price:   float64(i),
			Total:   float64(i),
			Bar:     fmt.Sprintf("BarBarBarBarBarBarBarBarBarBarBarBarBarBarBarBarBarBar_%d", i),
		})
	}
	return res
}

func generateMsgs(cnt int64) []*pb.DocStreamRequest {
	res := []*pb.DocStreamRequest{}
	var i int64
	for i = 0; i < cnt; i++ {
		res = append(res, &pb.DocStreamRequest{
			Rows:             generateRows(),
			SupplyId:         i,
			SellerId:         i,
			ShelfLife:        fmt.Sprintf("ShelfLife_%d", i),
			IdempotencyToken: fmt.Sprintf("IdIdempotencyTokeIdempotencyTokeIdempotencyTokeIdempotencyTokeIdempotencyTokeempotencyToken_%d", i),
			Address:          fmt.Sprintf("AATinTinTinTinTinTinTinTinTinddressAddressAddressAddressAddressddress_%d", i),
			Tin:              fmt.Sprintf("TTinTinTinTinin_%d", i),
			Flag:             true,
		})
	}
	return res
}

func sendDocsFull(client pb.DocsClient, cnt int64) {
	msgs := generateMsgs(cnt)
	totalTime := 0.0
	for _, m := range msgs {
		startTime := time.Now()
		stream, err := client.SendDocFull(context.Background())
		if err != nil {
			fmt.Printf("failed to connect: %s\n", err)
		}
		rows := m.Rows
		m.Rows = nil
		err = stream.Send(m)
		if err != nil {
			fmt.Printf("Failed with error: %s", err)
			return
		}
		m = &pb.DocStreamRequest{}
		for row_num := range rows {
			m.Rows = rows[row_num : row_num+1]
			err := stream.Send(m)
			if err != nil {
				fmt.Printf("Failed with error: %s", err)
				return
			}
		}
		res, err := stream.CloseAndRecv()
		endTime := time.Now()
		totalTime += endTime.Sub(startTime).Seconds()
		fmt.Printf("done. sent: %d, err: %s\n", res.TotarRowsCount, err)
	}
	fmt.Printf("All docs sent. avg time per doc: %f\n", totalTime/float64(len(msgs)))
}

func makeDocRequest(doc *pb.DocStreamRequest) *pb.DocOrRowRequest {
	return &pb.DocOrRowRequest{Msg: &pb.DocOrRowRequest_Doc{Doc: doc}}
}

func makeRowRequest(row *pb.Row) *pb.DocOrRowRequest {
	return &pb.DocOrRowRequest{Msg: &pb.DocOrRowRequest_Row{Row: row}}
}

func sendDocVariadic(client pb.DocsClient, cnt int64) {
	msgs := generateMsgs(cnt)
	totalTime := 0.0
	for _, m := range msgs {
		startTime := time.Now()
		stream, err := client.SendDocVariadic(context.Background())
		if err != nil {
			fmt.Printf("failed to connect: %s\n", err)
		}
		rows := m.Rows
		m.Rows = nil
		err = stream.Send(makeDocRequest(m))
		if err != nil {
			fmt.Printf("failed with error: %s", err)
			return
		}
		fmt.Println("Sent first message")
		for _, row := range rows {
			ms := makeRowRequest(row)
			err := stream.Send(ms)
			if err != nil {
				fmt.Printf("failed with error: %s", err)
				return
			}
		}
		res, err := stream.CloseAndRecv()
		endTime := time.Now()
		totalTime += endTime.Sub(startTime).Seconds()
		fmt.Printf("done. sent: %d, err: %s\n", res.TotarRowsCount, err)
	}
	fmt.Printf("All docs sent. avg time per doc: %f\n", totalTime/float64(len(msgs)))
}

func main() {
	conn, err := grpc.Dial("localhost:8084", grpc.WithInsecure())
	if err != nil {
		fmt.Printf("Failed to dial: %s\n", err)
		return
	}
	client := pb.NewDocsClient(conn)
	// sendDocsFull(client, 200)
	sendDocVariadic(client, 200)
}
