// Where possible, share file, copy to correct location, unshare file.
package main

import (
	"database/sql"
	"log"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/api/drive/v3"
)

const (
	dstRootID = "1AVppxxxxxxxxxxxxxxxx"
	dstUser   = "xxxxxx@gmail.com"
)

func loadDsts(db *sql.DB) map[string]string {
	reply := make(map[string]string)
	rows, err := db.Query("select id, dstid from src where isFolder")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var dstID sql.NullString

		err = rows.Scan(&id, &dstID)
		if err != nil {
			log.Fatal(err)
		}
		if dstID.Valid {
			reply[id] = dstID.String
		}
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
	return reply
}

func chooseParent(parents string, dstMap map[string]string) string {
	for _, p := range strings.Split(parents, ",") {
		if dst, found := dstMap[p]; found {
			return dst
		}
	}
	return dstRootID
}

type shareable struct {
	id, name, parentID string
}

func loadShareables(db *sql.DB) []shareable {
	dstMap := loadDsts(db)
	var reply []shareable
	rows, err := db.Query("select id, name, parents from src where canShare and not isFolder and dstID is null")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, name, parents string

		err = rows.Scan(&id, &name, &parents)
		if err != nil {
			log.Fatal(err)
		}
		reply = append(reply, shareable{id: id, name: name, parentID: chooseParent(parents, dstMap)})
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
	return reply
}

func main() {
	db, err := sql.Open("sqlite3", "./files.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	dstDrive, err := getConn("credentials.json", "dst_token.json")
	if err != nil {
		log.Fatalf("get conn dst %v", err)
	}

	srcDrive, err := getConn("credentials.json", "src_token.json")
	if err != nil {
		log.Fatalf("get conn src %v", err)
	}

	shareables := loadShareables(db)
	total := len(shareables)
	for n, sh := range shareables {
		log.Printf("%d/%d %s\n", n, total, sh.name)
		req := &drive.Permission{
			Type:         "user",
			EmailAddress: dstUser,
			Role:         "reader",
		}
		perm, err := srcDrive.Permissions.Create(sh.id, req).Do()
		if err != nil {
			log.Printf("share:%s", err)
			continue
		}
		id, err := copy(dstDrive, sh)
		if err == nil {
			updateLocation(db, sh, id)
		} else {
			log.Printf("copy->%s", err)
		}
		unshare(srcDrive, sh, perm.Id)
	}
}

func copy(s *drive.Service, sh shareable) (string, error) {
	req := &drive.File{
		Name:    sh.name,
		Parents: []string{sh.parentID},
	}
	f, err := s.Files.Copy(sh.id, req).Do()
	if err != nil {
		return "", err
	}
	return f.Id, nil
}

func updateLocation(db *sql.DB, sh shareable, id string) {
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := tx.Prepare("UPDATE src SET dstID = ? WHERE id = ?")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()
	_, err = stmt.Exec(id, sh.id)

	if err != nil {
		log.Fatal(err)
	}
	tx.Commit()
}

func unshare(s *drive.Service, sh shareable, permID string) {
	if err := s.Permissions.Delete(sh.id, permID).Do(); err != nil {
		log.Fatalf("Permissions.Delete(%s,%s); err=%s", sh.id, permID)
	}
}
