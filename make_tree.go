// Mirror the folder structure from a DB to a destination Drive folder.
package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/api/drive/v3"
)

const dstRootID = "xxxxxxxxxxxxxxx"

var (
	names    = make(map[string]string)   // id -> name
	children = make(map[string][]string) // parentID -> [child]
	roots    []string                    // IDs with no parent
	missing  = make(map[string]bool)     // parents with no name
	dstIDs   = make(map[string]string)
)

func main() {
	db, err := sql.Open("sqlite3", "./files.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	s, err := getConn("credentials.json", "dst_token.json")
	if err != nil {
		log.Fatalf("get conn src %v", err)
	}

	rows, err := db.Query("select id, dstid, name, parents from src where isFolder ")
	if err != nil {
		log.Fatal(err)
	}
	dups := 0
	defer rows.Close()
	for rows.Next() {
		var id, name, csvParents string
		var dstID sql.NullString

		err = rows.Scan(&id, &dstID, &name, &csvParents)
		if err != nil {
			log.Fatal(err)
		}
		if _, found := names[id]; found {
			dups++
		}
		names[id] = name
		if dstID.Valid {
			dstIDs[id] = dstID.String
		}
		if csvParents == "" {
			roots = append(roots, id)
			continue
		}
		parents := strings.Split(csvParents, ",")
		p := parents[0]
		children[p] = append(children[p], id)
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
	for p := range children {
		if _, found := names[p]; !found {
			missing[p] = true
		}
	}

	for m := range missing {
		names[m] = "MISSING " + m
		tree(db, s, dstRootID, m, "")
	}

	for _, r := range roots {
		tree(db, s, dstRootID, r, "")
	}

	fmt.Printf("%d dups, %d roots, %d missing\n", dups, len(roots), len(missing))
}

func storeDstID(db *sql.DB, src, dst string) {
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := tx.Prepare("UPDATE src SET dstID = ? WHERE id = ?")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()
	_, err = stmt.Exec(dst, src)

	if err != nil {
		log.Fatal(err)
	}
	tx.Commit()
}

func makeFolder(s *drive.Service, parentID, name string) string {
	req := &drive.File{
		Name:     name,
		MimeType: "application/vnd.google-apps.folder",
		Parents:  []string{parentID},
	}
	f, err := s.Files.Create(req).Do()
	if err != nil {
		log.Fatalf("Create parent:%q, req:%#v, err:%s", parentID, req, err)
	}
	return f.Id
}

func tree(db *sql.DB, s *drive.Service, dstRoot, id, indent string) {
	dstID := dstIDs[id]
	created := ""
	if dstID == "" {
		dstID = makeFolder(s, dstRoot, names[id])
		storeDstID(db, id, dstID)
		created = "*"
	}

	fmt.Printf("%s%s %s\n", indent, names[id], created)

	for _, c := range children[id] {
		tree(db, s, dstID, c, indent+"  ")
	}
}
