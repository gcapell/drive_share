// List the files/folders from a Google Drive into a DB.
package main

import (
	"context"
	"database/sql"
	"log"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/api/drive/v3"
)

const folderMimeType = "application/vnd.google-apps.folder"

func main() {
	log.Println("open DB")
	db, err := sql.Open("sqlite3", "./files.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	sqlStmt := `
	create table src (
		id text not null primary key,
		name text not null,
		isFolder bool not null,
		parents text not null,
		canShare bool not null,
		canCopy bool not null,
		dstId text
	);
	`
	log.Println("create table")
	_, err = db.Exec(sqlStmt)
	if err != nil {
		log.Fatalf("%q: %s\n", err, sqlStmt)
	}

	log.Println("connect to Drive")
	src, err := getConn("credentials.json", "src_token.json")
	if err != nil {
		log.Fatalf("get conn src %v", err)
	}

	log.Println("list")
	err = src.Files.List().
		IncludeItemsFromAllDrives(true).
		SupportsAllDrives(true).
		PageSize(1000).
		Fields("nextPageToken, files(id,name,parents,mimeType,capabilities/canShare,capabilities/canCopy)").
		Pages(context.TODO(), func(r *drive.FileList) error { return store(db, r) })
	if err != nil {
		log.Fatalf("List err %s", err)
	}
	log.Println("done")
}

func store(db *sql.DB, r *drive.FileList) error {
	log.Printf("store %d", len(r.Files))

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := tx.Prepare("insert into src(id, name, isFolder, parents, canShare, canCopy) values(?, ?, ?, ?, ?, ?)")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()
	for _, f := range r.Files {
		_, err = stmt.Exec(f.Id, f.Name, f.MimeType == folderMimeType, strings.Join(f.Parents, ","), f.Capabilities.CanShare, f.Capabilities.CanCopy)
		if err != nil {
			log.Fatal(err)
		}
	}
	tx.Commit()
	return nil
}
