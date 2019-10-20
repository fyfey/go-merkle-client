package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"fyfe.io/merkle"
	pb "fyfe.io/merkle"

	"google.golang.org/grpc"
)

func main() {

	var serverAddr string
	flag.StringVar(&serverAddr, "addr", "127.0.0.1:9999", "Server address")

	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewMerkleClient(conn)

	metadata, err := client.GetMetadata(context.Background(), &pb.Empty{})
	if err != nil {
		log.Fatal(err)
	}

	file, err := os.Create(metadata.Filename)
	if err != nil {
		log.Fatalf("Failed to open file")
	}
	defer file.Close()

	for i := 0; i < int(metadata.Parts); i++ {
		part, err := client.GetPart(context.Background(), &merkle.PartRequest{Idx: int32(i)})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Part: %v\n", part)
		fmt.Printf("Hash: %s\n", pb.Hash(part.Data))
		fmt.Printf("OK? %v\n", Prove(part.Proof, pb.Hash(part.Data)))

		if !Prove(part.Proof, pb.Hash(part.Data)) {
			log.Fatalf("Part %d failed merkle proof check\n", part.Idx)
		}
		offset := int64(int(part.Idx) * len(part.Data))
		fmt.Printf("Writing %d bytes @ %d", len(part.Data), offset)
		_, err = file.WriteAt(part.Data, offset)
		if err != nil {
			log.Fatal("Failed writing data to file")
		}
	}
}

func Prove(p *pb.Proof, ha string) bool {
	rootHash := p.MerkleRoot
	for i := 0; i < len(p.Nodes); i++ {
		if p.Nodes[i].Side == pb.Proof_ProofNode_RIGHT {
			ha = pb.Hash([]byte(ha + p.Nodes[i].Hash))
		} else {
			ha = pb.Hash([]byte(p.Nodes[i].Hash + ha))
		}
		fmt.Printf("#%03d: %s\n", i, ha)
	}

	return ha == rootHash
}
