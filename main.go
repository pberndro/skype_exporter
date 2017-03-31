package main 

import (
    "log"
    "net/http"
    "fmt"
    "flag"
    "io"
    "bufio"
    "bytes"
    "time"
)

import "database/sql"
import _ "github.com/denisenkom/go-mssqldb"

type t_metrics struct {
    lync_agents int
    //lync_xyz int   
}


var db *sql.DB 
var err error

var (
    
    stm_lync_agents = "SELECT COUNT(Agents.DisplayName) FROM rgsdyn.dbo.AgentGroupSignInStates AS States JOIN rgsconfig.dbo.Agents AS Agents ON Agents.ID = States.AgentId JOIN rgsconfig.dbo.AgentGroups AS Groups ON Groups.ID = States.GroupId WHERE Name like 'IT-Hotline-First-Level' and State = '1'"
    
    listenAddress = flag.String("web.listen-address", ":9200", "Address on which to expose metrics and web interface.")
    metricsPath = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
    
    //flags for sql connectionString..
    connectionString = flag.String("db.connectionString", "", "database connectionString.")
    dbname = flag.String("db.name","","database name")
    dbuser = flag.String("db.user","","username")
    dbpassword = flag.String("db.password","","database password")
    dbserver = flag.String("db.host","","hostname")
)


func startServer() {
    fmt.Printf("Starting Skype for Business exporter \n")
    
    err= db_init()
    if( err!= nil) {
        log.Println(err)        
        return
    }
    http.HandleFunc(*metricsPath, errorHandler(handleMetricsRequest))

    if( err!= nil) {
        log.Println(err)
        return
    }   
    fmt.Printf("Listening for %s on %s\n", *metricsPath, *listenAddress)
    log.Fatal(http.ListenAndServe(*listenAddress, nil))
}

func errorHandler(f func(io.Writer, *http.Request) error) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        var buf bytes.Buffer
        wr := bufio.NewWriter(&buf)
        err := f(wr, r)
        wr.Flush()

        if err != nil {
            log.Println(err)
            http.Error(w, err.Error(), http.StatusInternalServerError)
        }

        _, err = w.Write(buf.Bytes())

        if err != nil {
            log.Println(err)
        }
    }
}

func handleMetricsRequest(w io.Writer, r *http.Request) error {
    if err != nil {
        return err
    }

    err= get_metrics(w)
    return err
}

func get_metrics(w io.Writer) (err error){
    var metrics t_metrics

    metrics, err = db_metrics()

    for err!= nil {
        fmt.Printf("Connection refused for %s on %s\nWaiting for reconnect\n", *metricsPath, *listenAddress)
        time.Sleep(10 * time.Second)
        err= nil
        metrics, err = db_metrics()
        if err== nil {
            fmt.Printf("Listening again for %s on %s\n", *metricsPath, *listenAddress)
        }
    }

    metrics.outf(w)
    return nil
}

func db_stm(stm string) (count int, err error) {
    rows, err := db.Prepare(stm)
    if err != nil {
        panic(err.Error())
    }
    defer rows.Close()

    err = rows.QueryRow().Scan(&count)
    if err != nil {
        panic(err.Error())
    }
    return count, err
}

func db_init() (err error) {
    db, err = sql.Open("mssql", *connectionString)
    if err != nil {
        return err
    }

    err = db.Ping()
    if err!= nil {
        return  err
    }

    return nil
}

func db_metrics() (m t_metrics, err error) {
    err = db.Ping()
    if err!= nil {
        return m, err
    }

    m.lync_agents,err= db_stm(stm_lync_agents)
    if err!= nil {
        return m, err
    }

    return m, nil
}

func ( m *t_metrics) outf(w io.Writer) (err error) {
    fmt.Fprintf(w, "lync_agents %d\n", m.lync_agents)
    return
}

func main() {
    flag.Parse()
    startServer()
}
