package main

import (
	"crypto/tls"
	"fmt"
	"os"
	"github.com/stargate/stargate-grpc-go-client/stargate/pkg/auth"
	"github.com/stargate/stargate-grpc-go-client/stargate/pkg/client"
	pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var stargateClient *client.StargateClient

func main() {
    	fmt.Println("Begin .....!")

	// Astra DB configuration
	const astra_uri = "c2a8eb6c-312a-4903-b296-0945a0b50591-us-east-1.apps.astra.datastax.com:443";
	const bearer_token = "AstraCS:TBIwGbRWytSBEsyUBxrvstwz:717221911013217a4cf5d8944df761ead1e05f9158def21f75e86a1f4fc9796c";

	// Create connection with authentication
	// For Astra DB:
	config := &tls.Config{
  		InsecureSkipVerify: false,
	}

	conn, err := grpc.Dial(astra_uri, grpc.WithTransportCredentials(credentials.NewTLS(config)),
  		grpc.WithBlock(),
  		grpc.WithPerRPCCredentials(
  		auth.NewStaticTokenProvider(bearer_token),
  		),
	)
	fmt.Println("Connected .....!")

	stargateClient, err = client.NewStargateClientWithConn(conn)

	if err != nil {
  		fmt.Printf("error creating client %v", err)
  		os.Exit(1)
	}
	fmt.Printf("made client")

	batch := &pb.Batch{
		Type: pb.Batch_LOGGED,
		Queries: []*pb.BatchQuery{
			{
				Cql: "INSERT INTO content.zdmdemo (id, first, second) VALUES (100, 'Jane', 'Doe');",
			},
			{
				Cql: "INSERT INTO content.zdmdemo (id, first, second) VALUES (200, 'Serge', 'Provencio');",
			},
		},
	}

	_, err = stargateClient.ExecuteBatch(batch)
	if err != nil {
		fmt.Printf("error creating batch %v", err)
		return
	}

	fmt.Printf("insert executed\n")

	// For  Astra DB: SELECT the data to read from the table
	selectQuery := &pb.Query{
  		Cql: "SELECT first, second FROM content.zdmdemo;",
	}

	response, err := stargateClient.ExecuteQuery(selectQuery)
	if err != nil {
 	 	fmt.Printf("error executing query %v", err)
  		return
	}

	fmt.Printf("select executed\n")

	result := response.GetResultSet()

	rowCount := len(result.Rows)

	var i, j int
	for i = 0; i < rowCount; i++ {
		valueToPrint := ""
		columnCount := len(result.Rows[i].Values)
		for j = 0; j < columnCount; j++ {
			value, err := client.ToString(result.Rows[i].Values[j])
			if err != nil {
				fmt.Printf("error getting value %v", err)
				os.Exit(1)
			}
			valueToPrint += " "
			valueToPrint += value
		}
		fmt.Printf("%v \n", valueToPrint)
	}


}
