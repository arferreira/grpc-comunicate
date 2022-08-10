package main

import (
	"context"
	"fmt"
	"log"

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

	AddUser(client)
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
