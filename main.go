package main

import (   
 "github.com/tadvi/dbf"
 "fmt"
 "log"
 "strings"
 "os"
 "bufio"
 "io"
 "flag"
 "regexp"
)
var reg = regexp.MustCompile("[^0-9.,]+")


func getType( typeName string,  length uint8) string { 
	if (typeName == "D") { 
		return "[date]"
	}
	if (typeName == "C") { 
		return fmt.Sprintf("[varchar](%d)", length)
	}
	if (typeName == "N") {
		return fmt.Sprintf("[numeric](%d, %d)", length+2, 2)
	}
	if (typeName == "L"){
		return "[bit]"
	} 
	log.Fatal("unknown type", typeName)
	return "" 
}



func getLine(name string, typeName string, length uint8) string {
	return fmt.Sprintf("[%s] %s NULL", name, getType(typeName, length))
}
func convertValue(inputValue string, typeName string, length uint8) string{ 
	if typeName == "L" { 
		if strings.ToLower(inputValue) == "t"  || inputValue == "1"  || strings.ToLower(inputValue) == "y" { 
			return "0"
		} else { 
			return "1" 
		}
	}
	if typeName == "N" {
		
		inputValue = reg.ReplaceAllString(inputValue, "")
		
		switch inputValue { 
			case 
				"0",
				"",
				".",
				",":
			inputValue =  "0"
		}
		return inputValue
	}
	return fmt.Sprintf("'%s'", inputValue)
}

func main() {
	var DbfFileName = flag.String("dbf", "database.dbf", "DBF file")
	var TableName = flag.String("table", "dbf", "table name")
	var OutputFile = flag.String("output", "data.sql", "output (Transact-SQL script) file name ")
	var MakeDDL = flag.Bool("ddl", true, "generate DDL (CREATE TABLE) statements")
	var MakeData = flag.Bool("data", true, "generate data (INSERT) statements")
	flag.Parse()

	dbfTable, err := dbf.LoadFile(*DbfFileName)
	if(err != nil ){
		log.Fatal(err)
	}
	var DbFields []dbf.DbfField = dbfTable.Fields()
	var fields  []string
	var fieldNames []string 
	var fname string
	for _, field := range DbFields {
		fname = strings.Replace(field.Name, "\000", "", -1) //sqlcmd nepatīk 0-terminēti stringi
		fields = append(fields, (getLine(fname, field.Type, field.Length)))
		fieldNames = append(fieldNames, fmt.Sprintf("[%s]", fname))
	}
	var CreateSQL = fmt.Sprintf("SET NOCOUNT ON; DROP TABLE IF EXISTS %s; \n CREATE TABLE %s \n(%s); \n GO \n ", *TableName, *TableName, strings.Join(fields, ", \n"));

	var RowCount = dbfTable.NumRecords()
	var InsertRows  []string
	f, err := os.Create(*OutputFile)
	if(err != nil ){
		log.Fatal(err)
	}

    defer f.Close()

    w := bufio.NewWriter(f)
	if (*MakeDDL){
		_, err = io.WriteString(w, CreateSQL)
	}

	if *MakeData { 
		for  i:=0; i < RowCount; i++ {
			rowValues := []string{}
			for _, field := range DbFields { 
				rowValues = append(rowValues, convertValue(dbfTable.FieldValueByName(i, field.Name), field.Type, field.Length))
			}
			InsertRows = append(InsertRows, fmt.Sprintf("(%s)", strings.Join(rowValues, ", " )))
			if((i%1000 == 0) || (i == RowCount-1) ){ //sqlcmd crashes with too big transactions, so we make every insert into a transaction
				_, err = io.WriteString(w, fmt.Sprintf("INSERT INTO [%s] \n (%s) \n VALUES %s; \n GO \n", *TableName, strings.Join(fieldNames, ", "), strings.Join(InsertRows, ", \n"), ))
				if(err != nil ){
					log.Fatal(err)
				}
				InsertRows = []string{}
			}
		}
	}
	w.Flush()
}