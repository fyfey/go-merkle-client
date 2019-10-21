package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

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

	var wg sync.WaitGroup
	wg.Add(int(metadata.Parts))
	for i := 0; i < int(metadata.Parts); i++ {
		func() {
			part, err := client.GetPart(context.Background(), &merkle.PartRequest{Idx: int32(i)})
			if err != nil {
				log.Fatal(err)
			}
			if !prove(part.Proof, pb.Hash(part.Data)) {
				log.Fatalf("Part %d failed merkle proof check\n", part.Idx)
			}
			offset := int64(int(part.Idx) * int(metadata.ChunkSize))
			fmt.Printf("Writing %d bytes @ %d - %x", len(part.Data), offset, part.Data)
			_, err = file.WriteAt(part.Data, offset)
			if err != nil {
				log.Fatal("Failed writing data to file")
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func prove(p *pb.Proof, ha string) bool {
	rootHash := p.MerkleRoot
	for i := 0; i < len(p.Nodes); i++ {
		if p.Nodes[i].Side == pb.Proof_ProofNode_RIGHT {
			ha = pb.Hash([]byte(ha + p.Nodes[i].Hash))
		} else {
			ha = pb.Hash([]byte(p.Nodes[i].Hash + ha))
		}
	}

	return ha == rootHash
}
