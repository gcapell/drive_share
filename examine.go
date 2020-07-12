// Given Drive fileID, show details of file and parent folders.
package main

import (
	"fmt"
	"log"
	"os"

	"google.golang.org/api/drive/v3"
)

func main() {
	src, err := getConn("credentials.json", "src_token.json")
	if err != nil {
		log.Fatalf("get conn  %v", err)
	}
	for _, f := range os.Args[1:] {
		showFile(src, f, "")
	}

}

func showFile(s *drive.Service, id string, ind string) {
	f, err := s.Files.Get(id).SupportsAllDrives(true).Fields("*").Do()
	if err != nil {
		log.Fatalf("Unable to retrieve %s: %v", id, err)
	}
	fmt.Printf("%s%s(%s)\n", ind, f.Name, f.Id)
	for _, p := range f.Parents {
		showFile(s, p, ind+"  ")
	}
}
