package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/arferreira/grpc-comunicate/pb"
	"google.golang.org/grpc"
)

func main() {
	connection, err := grpc.Dial("localhost:50051", grpc.WithInsecure())

	if err != nil {
		log.Fatalf("Could not connect to server: %v", err)
	}
	defer connection.Close()

	client := pb.NewUserServiceClient(connection)

	// AddUser(client)
	// AddUserVerbose(client)
	// AddUsers(client)
	AddUserStreamBoth(client)
}

func AddUser(client pb.UserServiceClient) {
	req := &pb.User{
		Id:    "0",
		Name:  "Mariana",
		Email: "marian@gmail.com",
	}

	res, err := client.AddUser(context.Background(), req)

	if err != nil {
		log.Fatalf("Could not make grpc request: %v", err)
	}
	fmt.Println(res)
}

func AddUserVerbose(client pb.UserServiceClient) {
	req := &pb.User{
		Id:    "0",
		Name:  "Mariana",
		Email: "marian@gmail.com",
	}

	responseStream, err := client.AddUserVerbose(context.Background(), req)

	if err != nil {
		log.Fatalf("Could not make grpc request: %v", err)
	}

	for {
		stream, err := responseStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Could not receive msg: %v", err)
		}
		fmt.Println("Status", stream.Status)
	}
}

func AddUsers(client pb.UserServiceClient) {
	reqs := []*pb.User{
		&pb.User{
			Id:    "a1",
			Name:  "Antonio",
			Email: "antonio@gmail.com",
		},
		&pb.User{
			Id:    "a2",
			Name:  "Antonio",
			Email: "antonio@gmail.com",
		},
		&pb.User{
			Id:    "a3",
			Name:  "Antonio",
			Email: "antonio@gmail.com",
		},
		&pb.User{
			Id:    "a4",
			Name:  "Antonio",
			Email: "antonio@gmail.com",
		},
	}
	stream, err := client.AddUsers(context.Background())

	if err != nil {
		log.Fatalf("Error create request: %v", err)
	}

	for _, req := range reqs {
		stream.Send(req)
		time.Sleep(time.Second * 2)
	}

	res, err := stream.CloseAndRecv()

	if err != nil {
		log.Fatalf("Error receive response: %v", err)
	}

	fmt.Println(res)

}

func AddUserStreamBoth(client pb.UserServiceClient) {
	stream, err := client.AddUserStreamBoth(context.Background())

	if err != nil {
		log.Fatalf("Error create request: %v", err)
	}

	reqs := []*pb.User{
		&pb.User{
			Id:    "a1",
			Name:  "Antonio",
			Email: "antonio@gmail.com",
		},
		&pb.User{
			Id:    "a2",
			Name:  "Antonio",
			Email: "antonio@gmail.com",
		},
		&pb.User{
			Id:    "a3",
			Name:  "Antonio",
			Email: "antonio@gmail.com",
		},
		&pb.User{
			Id:    "a4",
			Name:  "Antonio",
			Email: "antonio@gmail.com",
		},
	}

	wait := make(chan int)

	// goroutine to handle stream both
	go func() {
		for _, req := range reqs {
			fmt.Println("Sending user: ", req.Name)
			stream.Send(req)
			time.Sleep(time.Second * 2)
		}
		stream.CloseSend()
	}()

	// Applying concurrency with goroutine to receive stream
	go func() {
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("Error ocurring on receiving data: %v", err)
				break
			}
			fmt.Printf("Receiving user %v with status %v", res.GetUser().GetName(), res.GetStatus())
		}
		close(wait)
	}()

	<-wait
}
