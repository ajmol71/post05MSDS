package post05MSDS

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"strconv"

	_ "github.com/lib/pq"
)

// Connection details
var (
	Hostname = ""
	Port     = 2345
	Username = ""
	Password = ""
	Database = ""
)


// Course Structure
type MSDSCourse struct {
	CID         string
	CNAME	    string
	CPREREQ     string
}

func openConnection() (*sql.DB, error) {
	// connection string
	conn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		Hostname, Port, Username, Password, Database)

	// open database
	db, err := sql.Open("postgres", conn)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// The function returns the course ID of the class
// -1 if the user does not exist
func exists(username string) string {
	username = strings.ToLower(username)

	db, err := openConnection()
	if err != nil {
		fmt.Println(err)
		return "-1"
	}
	defer db.Close()

	courseID := "-1"
	statement := fmt.Sprintf(`SELECT "cid" FROM "msdscoursecatalog" where cid = '%s'`, username)
	rows, err := db.Query(statement)

	for rows.Next() {
		var id int
		err = rows.Scan(&id)
		if err != nil {
			fmt.Println("Scan", err)
			return "-1"
		}
		courseID = strconv.Itoa(id)
	}
	defer rows.Close()
	return courseID
}

// AddCourse adds a new course to the database
// Returns new course ID
// -1 if there was an error
func AddCourse(d MSDSCourse) string {
	db, err := openConnection()
	if err != nil {
		fmt.Println(err)
		return "-1"
	}
	defer db.Close()

	courseID := exists(d.CID)
	if courseID != "-1" {
		fmt.Println("Course already exists:", d.CID)
		return "-1"
	}

	insertStatement := `insert into "msdscoursecatalog" ("cid") values ($1)`
	_, err = db.Exec(insertStatement, d.CID)
	if err != nil {
		fmt.Println(err)
		return "-1"
	}

	courseID = exists(d.CID)
	if courseID == "-1" {
		return courseID
	}

	insertStatement = `insert into "msdscoursecatalog" ("cid", "cname", "cprereq")
	values ($1, $2, $3)`
	_, err = db.Exec(insertStatement, courseID, d.CNAME, d.CPREREQ)
	if err != nil {
		fmt.Println("db.Exec()", err)
		return "-1"
	}

	return courseID
}

// DeleteCourse deletes an existing course
func DeleteCourse(id string) error {
	db, err := openConnection()
	if err != nil {
		return err
	}
	defer db.Close()

	// Does the ID exist?
	statement := fmt.Sprintf(`SELECT "cid" FROM "msdscoursecatalog" WHERE cid = %d`, id)
	rows, err := db.Query(statement)

	var cid string
	for rows.Next() {
		err = rows.Scan(&cid)
		if err != nil {
			return err
		}
	}
	defer rows.Close()

	if exists(cid) != id {
		return fmt.Errorf("Course with ID %d does not exist", id)
	}


	// deleteStatement := `delete from "MSDSCourseCatalog" where courseid=$1`
	// _, err = db.Exec(deleteStatement, id)
	// if err != nil {
	// 	return err
	// }

	// Delete from catalog
	deleteStatement := `DELETE FROM "msdscoursecatalog" WHERE cid = $1`
	_, err = db.Exec(deleteStatement, id)
	if err != nil {
		return err
	}

	return nil
}

// ListCourses lists all courses in the database
func ListCourses() ([]MSDSCourse, error) {
	Data := []MSDSCourse{}
	db, err := openConnection()
	if err != nil {
		return Data, err
	}
	defer db.Close()

	rows, err := db.Query(`SELECT "cid","cname","cprereq" FROM "msdscoursecatalog"`)
	if err != nil {
		return Data, err
	}

	for rows.Next() {
		var cid string
		var cname string
		var cprereq string
		err = rows.Scan(&cid, IFNULL(&cname, ""), IFNULL(&cprereq, ""))
		temp := MSDSCourse{CID: cid, CNAME: cname, CPREREQ: cprereq}
		Data = append(Data, temp)
		if err != nil {
			return Data, err
		}
	}
	defer rows.Close()
	return Data, nil
}

// UpdateCourse is for updating an existing course
func UpdateCourse(d MSDSCourse) error {
	db, err := openConnection()
	if err != nil {
		return err
	}
	defer db.Close()

	courseID := exists(d.CID)
	if courseID == "-1" {
		return errors.New("Course does not exist")
	}
	d.CID = courseID
	updateStatement := `update "msdscoursecatalog" set "cname"=$1, "cprereq"=$2, where "cid"=$3`
	_, err = db.Exec(updateStatement, d.CNAME, d.CPREREQ, d.CID)
	if err != nil {
		return err
	}

	return nil
}
