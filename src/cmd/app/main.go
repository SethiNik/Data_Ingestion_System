package main

/*
===========================================================
 FINTECH INGESTION PLATFORM
===========================================================

This service performs:

1. Fetch HTML table from URL
2. Parse table rows and columns
3. Infer schema automatically
4. Normalize column names safely
5. Publish ingestion jobs to Kafka
6. Consumer persists rows to DB
7. Track ingestion progress
8. Provide ingestion logs
9. Provide DB explorer APIs

Frontend dashboard interacts with this service.

===========================================================
*/


import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/PuerkitoBio/goquery"
	"github.com/google/uuid"
	_ "github.com/go-sql-driver/mysql"
)

///////////////////////////////////////////////////////////
//////////////////// GLOBALS /////////////////////////////
///////////////////////////////////////////////////////////

var producer sarama.SyncProducer
var db *sql.DB

///////////////////////////////////////////////////////////
//////////////////// DATA TYPES //////////////////////////
///////////////////////////////////////////////////////////

type Preview struct {
	Columns []string          `json:"columns"`
	Types   map[string]string `json:"types"`
	Rows    [][]string        `json:"rows"`
}

type IngestRequest struct {
	URL   string `json:"url"`
	Table string `json:"table"`
	Mode  string `json:"mode"`
	Dedup bool   `json:"dedup"`
}

///////////////////////////////////////////////////////////
/////////////////////// MAIN /////////////////////////////
///////////////////////////////////////////////////////////

func main() {

	setupKafka()
	setupDB()
	ensureMetaTables()

	go startConsumer()

	http.Handle("/", http.FileServer(http.Dir("./web")))
	http.HandleFunc("/preview", previewHandler)
	http.HandleFunc("/ingest", ingestHandler)
	http.HandleFunc("/tables", tablesHandler)
	http.HandleFunc("/table", tableHandler)
	http.HandleFunc("/job_status", jobStatusHandler)
	http.HandleFunc("/job_logs", jobLogsHandler)

	fmt.Println("Server running")
	http.ListenAndServe(":"+os.Getenv("APP_PORT"), nil)
}

///////////////////////////////////////////////////////////
//////////////////// SETUP ///////////////////////////////
///////////////////////////////////////////////////////////

func setupKafka() {

	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true

	p, err := sarama.NewSyncProducer(
		[]string{os.Getenv("KAFKA_BROKER")},
		cfg,
	)
	if err != nil {
		panic(err)
	}

	producer = p
}

func setupDB() {

	dsn := os.Getenv("DB_USER") + ":" +
		os.Getenv("DB_PASSWORD") +
		"@tcp(" + os.Getenv("DB_HOST") +
		":3306)/" + os.Getenv("DB_NAME")

	var err error

	for i := 0; i < 20; i++ {

		db, err = sql.Open("mysql", dsn)
		if err == nil && db.Ping() == nil {
			fmt.Println("DB connected")
			return
		}

		fmt.Println("Waiting for DB...")
		time.Sleep(3 * time.Second)
	}

	panic("DB unavailable")
}

func ensureMetaTables() {

	db.Exec(`
	CREATE TABLE IF NOT EXISTS ingestion_jobs(
		id VARCHAR(64) PRIMARY KEY,
		table_name TEXT,
		total_rows INT,
		inserted_rows INT,
		status TEXT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`)

	db.Exec(`
	CREATE TABLE IF NOT EXISTS ingestion_logs(
		id INT AUTO_INCREMENT PRIMARY KEY,
		job_id VARCHAR(64),
		message TEXT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`)
}

///////////////////////////////////////////////////////////
//////////////////// HANDLERS ////////////////////////////
///////////////////////////////////////////////////////////

func previewHandler(w http.ResponseWriter, r *http.Request) {

	var req struct{ URL string }
	json.NewDecoder(r.Body).Decode(&req)

	p, err := parseTable(req.URL)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	json.NewEncoder(w).Encode(p)
}

func ingestHandler(w http.ResponseWriter, r *http.Request) {

	var req IngestRequest
	json.NewDecoder(r.Body).Decode(&req)

	p, err := parseTable(req.URL)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	jobID := uuid.New().String()

	db.Exec(`
	INSERT INTO ingestion_jobs
	(id, table_name, total_rows, inserted_rows, status)
	VALUES (?, ?, ?, 0, 'running')`,
		jobID, req.Table, len(p.Rows))

	payload := map[string]interface{}{
		"preview": p,
		"table":   req.Table,
		"mode":    req.Mode,
		"dedup":   req.Dedup,
		"job_id":  jobID,
	}

	b, _ := json.Marshal(payload)

	producer.SendMessage(&sarama.ProducerMessage{
		Topic: "table_rows",
		Value: sarama.ByteEncoder(b),
	})

	w.Write([]byte(jobID))
}

///////////////////////////////////////////////////////////
//////////////////// FETCH + PARSE ///////////////////////
///////////////////////////////////////////////////////////

func fetchDocument(url string) (*goquery.Document, error) {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	return goquery.NewDocumentFromReader(resp.Body)
}

func parseTable(url string) (Preview, error) {

	doc, err := fetchDocument(url)
	if err != nil {
		return Preview{}, fmt.Errorf("failed to fetch document: %w", err)
	}

	var cols []string
	var rows [][]string

	table := doc.Find("table").First()
	if table.Length() == 0 {
		return Preview{}, fmt.Errorf("no table found in HTML")
	}

	table.Find("tr").Each(func(i int, tr *goquery.Selection) {

		var row []string

		// MINIMAL FIX: Check if row has headers vs data
		if tr.Find("th").Length() > 0 {
			// Header row
			tr.Find("th").Each(func(_ int, th *goquery.Selection) {
				// MINIMAL FIX: Look for .dt-column-title first (DataTables), fallback to full text
				text := th.Find(".dt-column-title").First().Text()
				if text == "" {
					text = th.Text()
				}
				row = append(row, strings.TrimSpace(text))
			})
			if i == 0 {
				cols = row
			}
		} else {
			// Data row
			tr.Find("td").Each(func(_ int, td *goquery.Selection) {
				row = append(row, strings.TrimSpace(td.Text()))
			})
			if len(row) > 0 {
				rows = append(rows, row)
			}
		}
	})

	if len(cols) == 0 {
		return Preview{}, fmt.Errorf("no columns found in table")
	}

	if len(rows) == 0 {
		return Preview{}, fmt.Errorf("no data rows found in table")
	}

	cols = normalizeColumns(cols)

	fmt.Printf("✓ Parsed table: %d columns × %d rows\n", len(cols), len(rows))
	fmt.Printf("✓ Columns: %v\n", cols)

	return Preview{
		Columns: cols,
		Types:   inferTypes(cols, rows),
		Rows:    rows,
	}, nil
}

///////////////////////////////////////////////////////////
//////////////// COLUMN NORMALIZATION ////////////////////
///////////////////////////////////////////////////////////

var invalidChars = regexp.MustCompile(`[^a-zA-Z0-9_]+`)

func normalizeColumns(cols []string) []string {

	seen := map[string]int{}
	result := make([]string, len(cols))

	for i, c := range cols {

		name := strings.ToLower(c)
		name = strings.ReplaceAll(name, " ", "_")
		name = invalidChars.ReplaceAllString(name, "")
		name = strings.Trim(name, "_")

		if name == "" {
			name = fmt.Sprintf("col_%d", i)
		}

		if count, ok := seen[name]; ok {
			count++
			seen[name] = count
			name = fmt.Sprintf("%s_%d", name, count)
		} else {
			seen[name] = 1
		}

		result[i] = name
	}

	return result
}

///////////////////////////////////////////////////////////
//////////////////// TYPE INFERENCE //////////////////////
///////////////////////////////////////////////////////////

func cleanForInference(v string) string {

	v = strings.TrimSpace(v)

	v = strings.ReplaceAll(v, ",", "")
	v = strings.ReplaceAll(v, "$", "")
	v = strings.ReplaceAll(v, "%", "")
	v = strings.ReplaceAll(v, "–", "-")

	if i := strings.Index(v, "["); i != -1 {
		v = v[:i]
	}

	return strings.TrimSpace(v)
}

var dateLayouts = []string{
	"2006-01-02",
	"02/01/2006",
	"01/02/2006",
	"02 Jan 2006",
	"Jan 2, 2006",
}

var dateTimeLayouts = []string{
	time.RFC3339,
	"2006-01-02 15:04:05",
	"2006-01-02 15:04",
	"02 Jan 2006 15:04",
}

func matchesAnyLayout(v string, layouts []string) bool {

	for _, l := range layouts {
		if _, err := time.Parse(l, v); err == nil {
			return true
		}
	}
	return false
}

func inferTypes(cols []string, rows [][]string) map[string]string {

	result := map[string]string{}

	for c := range cols {

		var intCount, floatCount, dateCount, dtCount, total int

		for _, r := range rows {

			if c >= len(r) {
				continue
			}

			val := cleanForInference(r[c])
			if val == "" {
				continue
			}

			total++

			if _, err := strconv.Atoi(val); err == nil {
				intCount++
			}

			if _, err := strconv.ParseFloat(val, 64); err == nil {
				floatCount++
			}

			if matchesAnyLayout(val, dateLayouts) {
				dateCount++
			}

			if matchesAnyLayout(val, dateTimeLayouts) {
				dtCount++
			}
		}

		if total == 0 {
			result[cols[c]] = "TEXT"
			continue
		}

		threshold := float64(total) * 0.8

		switch {
		case float64(intCount) >= threshold:
			result[cols[c]] = "INT"

		case float64(floatCount) >= threshold:
			result[cols[c]] = "FLOAT"

		case float64(dtCount) >= threshold:
			result[cols[c]] = "DATETIME"

		case float64(dateCount) >= threshold:
			result[cols[c]] = "DATE"

		default:
			result[cols[c]] = "TEXT"
		}
	}

	return result
}



///////////////////////////////////////////////////////////
//////////////////// KAFKA CONSUMER //////////////////////
///////////////////////////////////////////////////////////

func startConsumer() {

	cfg := sarama.NewConfig()

	consumer, _ := sarama.NewConsumer(
		[]string{os.Getenv("KAFKA_BROKER")},
		cfg,
	)

	pc, _ := consumer.ConsumePartition(
		"table_rows", 0, sarama.OffsetNewest,
	)

	for msg := range pc.Messages() {

		var payload map[string]interface{}
		json.Unmarshal(msg.Value, &payload)

		p := convertPreview(payload["preview"])
		table := payload["table"].(string)
		mode := payload["mode"].(string)
		dedup := payload["dedup"].(bool)
		jobID := payload["job_id"].(string)

		insertRows(p, table, mode, dedup, jobID)
	}
}

///////////////////////////////////////////////////////////
//////////////////// INSERTION ///////////////////////////
///////////////////////////////////////////////////////////

func cleanValue(v string) string {
	// Clean the value the same way we do for inference
	v = strings.TrimSpace(v)

	// Remove currency symbols and formatting
	v = strings.ReplaceAll(v, ",", "")
	v = strings.ReplaceAll(v, "$", "")
	v = strings.ReplaceAll(v, "£", "")
	v = strings.ReplaceAll(v, "€", "")
	v = strings.ReplaceAll(v, "%", "")
	v = strings.ReplaceAll(v, "–", "-")

	// Remove anything in brackets like "[citation needed]"
	if i := strings.Index(v, "["); i != -1 {
		v = v[:i]
	}

	return strings.TrimSpace(v)
}

func insertRows(p Preview, table, mode string, dedup bool, jobID string) {

	fmt.Printf("📊 Starting ingestion for table '%s' (mode: %s, rows: %d)\n", table, mode, len(p.Rows))

	if mode == "create" {
		db.Exec("DROP TABLE IF EXISTS " + table)
		fmt.Printf("🗑️  Dropped existing table '%s'\n", table)
	}

	create := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(", table)

	for _, c := range p.Columns {
		create += fmt.Sprintf("%s %s,", c, p.Types[c])
	}

	create = create[:len(create)-1] + ")"

	if _, err := db.Exec(create); err != nil {
		fmt.Printf("❌ Failed to create table: %v\n", err)
		db.Exec(`UPDATE ingestion_jobs SET status='failed' WHERE id=?`, jobID)
		return
	}

	fmt.Printf("✓ Created table schema\n")

	inserted := 0
	failed := 0

	for _, r := range p.Rows {

		query := fmt.Sprintf("INSERT IGNORE INTO %s VALUES(", table)

		for range r {
			query += "?,"
		}

		query = query[:len(query)-1] + ")"

		args := make([]interface{}, len(r))

		for i := range r {
			args[i] = cleanValue(r[i])
		}

		result, err := db.Exec(query, args...)
		if err != nil {
			failed++
			if failed <= 5 {
				fmt.Printf("⚠️  Row insert error: %v\n", err)
			}
			continue
		}

		rowsAffected, _ := result.RowsAffected()
		if rowsAffected > 0 {
			inserted++
		}

		if inserted%50 == 0 {
			db.Exec(`
			UPDATE ingestion_jobs
			SET inserted_rows=?
			WHERE id=?`,
				inserted, jobID)
			fmt.Printf("📝 Progress: %d/%d rows inserted\n", inserted, len(p.Rows))
		}
	}

	db.Exec(`
	UPDATE ingestion_jobs
	SET inserted_rows=?, status='completed'
	WHERE id=?`,
		inserted, jobID)

	fmt.Printf("✅ Ingestion complete: %d inserted, %d failed\n", inserted, failed)
}

///////////////////////////////////////////////////////////
//////////////////// JOB STATUS //////////////////////////
///////////////////////////////////////////////////////////

func jobStatusHandler(w http.ResponseWriter, r *http.Request) {

	id := r.URL.Query().Get("id")

	row := db.QueryRow(`
	SELECT total_rows, inserted_rows, status
	FROM ingestion_jobs WHERE id=?`, id)

	var total, inserted int
	var status string

	row.Scan(&total, &inserted, &status)

	json.NewEncoder(w).Encode(map[string]interface{}{
		"total":    total,
		"inserted": inserted,
		"status":   status,
	})
}

func jobLogsHandler(w http.ResponseWriter, r *http.Request) {

	id := r.URL.Query().Get("id")

	rows, _ := db.Query(`
	SELECT message, created_at
	FROM ingestion_logs
	WHERE job_id=?
	ORDER BY id DESC
	LIMIT 50`, id)

	var logs []map[string]string

	for rows.Next() {
		var msg, t string
		rows.Scan(&msg, &t)
		logs = append(logs, map[string]string{
			"time": t,
			"msg":  msg,
		})
	}

	json.NewEncoder(w).Encode(logs)
}

///////////////////////////////////////////////////////////
//////////////////// DB EXPLORER /////////////////////////
///////////////////////////////////////////////////////////

func tablesHandler(w http.ResponseWriter, r *http.Request) {

	rows, _ := db.Query("SHOW TABLES")

	var res []string

	for rows.Next() {
		var t string
		rows.Scan(&t)
		res = append(res, t)
	}

	json.NewEncoder(w).Encode(res)
}

func tableHandler(w http.ResponseWriter, r *http.Request) {
    name := r.URL.Query().Get("name")

    rows, err := db.Query("SELECT * FROM " + name + " LIMIT 200")
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    defer rows.Close()

    cols, err := rows.Columns()
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    var result []map[string]interface{}

    for rows.Next() {

        vals := make([]interface{}, len(cols))
        ptrs := make([]interface{}, len(cols))

        for i := range vals {
            ptrs[i] = &vals[i]
        }

        if err := rows.Scan(ptrs...); err != nil {
            continue
        }

        rowMap := make(map[string]interface{})

        for i, c := range cols {
            val := vals[i]

            switch v := val.(type) {

            case []byte:
                // MySQL often sends values as []byte
                s := string(v)

                if n, err := strconv.ParseInt(s, 10, 64); err == nil {
                    rowMap[c] = n
                } else if f, err := strconv.ParseFloat(s, 64); err == nil {
                    rowMap[c] = f
                } else {
                    rowMap[c] = s
                }

            default:
                rowMap[c] = v
            }
        }

        result = append(result, rowMap)
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(result)
}


///////////////////////////////////////////////////////////
//////////////////// HELPER //////////////////////////////
///////////////////////////////////////////////////////////

func convertPreview(v interface{}) Preview {

	b, _ := json.Marshal(v)

	var p Preview
	json.Unmarshal(b, &p)

	return p
}