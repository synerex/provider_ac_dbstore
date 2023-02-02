package main

import (
	// "encoding/json"
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	protoPC "github.com/synerex/proto_pcounter"

	"github.com/golang/protobuf/proto"
	"github.com/jackc/pgx/v4/pgxpool"
	api "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"

	sxutil "github.com/synerex/synerex_sxutil"
	//sxutil "local.packages/synerex_sxutil"

	"log"
	"sync"
)

// datastore provider provides Datastore Service.

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	local           = flag.String("local", "", "Local Synerex Server")
	mu              sync.Mutex
	version         = "0.01"
	baseDir         = "store"
	dataDir         string
	acMu            *sync.Mutex = nil
	acLoop          *bool       = nil
	ssMu            *sync.Mutex = nil
	ssLoop          *bool       = nil
	sxServerAddress string
	currentNid      uint64                  = 0 // NotifyDemand message ID
	mbusID          uint64                  = 0 // storage MBus ID
	storageID       uint64                  = 0 // storageID
	acClient        *sxutil.SXServiceClient = nil
	db              *pgxpool.Pool
	db_host         = os.Getenv("POSTGRES_HOST")
	db_name         = os.Getenv("POSTGRES_DB")
	db_user         = os.Getenv("POSTGRES_USER")
	db_pswd         = os.Getenv("POSTGRES_PASSWORD")
)

const layout = "2006-01-02T15:04:05.999999Z"
const layout_db = "2006-01-02 15:04:05.999"

func init() {
	// connect
	ctx := context.Background()
	addr := fmt.Sprintf("postgres://%s:%s@%s:5432/%s", db_user, db_pswd, db_host, db_name)
	print("connecting to " + addr + "\n")
	var err error
	db, err = pgxpool.Connect(ctx, addr)
	if err != nil {
		print("connection error: ")
		log.Println(err)
		log.Fatal("\n")
	}
	defer db.Close()

	// ping
	err = db.Ping(ctx)
	if err != nil {
		print("ping error: ")
		log.Println(err)
		log.Fatal("\n")
	}

	// create table
	_, err = db.Exec(ctx, `create table if not exists ac(id BIGSERIAL NOT NULL, time TIMESTAMP not null, aid INT not null, count INT not null, primary key(id))`)
	if err != nil {
		print("create table error: ")
		log.Println(err)
		log.Fatal("\n")
	}
}

func dbStore(ts time.Time, aid uint32, count int32) {

	// ping
	ctx := context.Background()
	err := db.Ping(ctx)
	if err != nil {
		print("ping error: ")
		log.Println(err)
		print("\n")
		// connect
		addr := fmt.Sprintf("postgres://%s:%s@%s:5432/%s", db_user, db_pswd, db_host, db_name)
		print("connecting to " + addr + "\n")
		db, err = pgxpool.Connect(ctx, addr)
		if err != nil {
			print("connection error: ")
			log.Println(err)
			print("\n")
		}
	}

	log.Printf("Storeing %v, %d, %d", ts.Format(layout_db), aid, count)
	result, err := db.Exec(ctx, `insert into ac(time, aid, count) values($1, $2, $3)`, ts.Format(layout_db), aid, count)

	if err != nil {
		print("exec error: ")
		log.Println(err)
		print("\n")
	} else {
		rowsAffected := result.RowsAffected()
		if err != nil {
			log.Println(err)
		} else {
			print(rowsAffected)
		}
	}

}

// called for each agent data.
func supplyACountCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {

	if sp.SupplyName == "PFlow Aggregate Supply" {
		acs := &protoPC.ACounters{}
		err := proto.Unmarshal(sp.Cdata.Entity, acs)
		if err == nil {
			for _, ac := range acs.Acs {
				dbStore(ac.Ts.AsTime(), uint32(ac.AreaId), ac.Count)
			}
		} else {
			log.Printf("Unmarshaling err AC: %v", err)
		}
	} else {
		log.Printf("Received Unknown (%4d bytes, %s)", len(sp.Cdata.Entity), sp.SupplyName)
	}
}

func main() {
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)
	log.Printf("AC-dbstore(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)

	channelTypes := []uint32{pbase.AREA_COUNTER_SVC, pbase.STORAGE_SERVICE}

	var rerr error
	sxServerAddress, rerr = sxutil.RegisterNode(*nodesrv, "ACdbstore", channelTypes, nil)

	if rerr != nil {
		log.Fatal("Can't register node:", rerr)
	}
	if *local != "" { // quick hack for AWS local network
		sxServerAddress = *local
	}
	log.Printf("Connecting SynerexServer at [%s]", sxServerAddress)

	wg := sync.WaitGroup{} // for syncing other goroutines

	client := sxutil.GrpcConnectServer(sxServerAddress)

	if client == nil {
		log.Fatal("Can't connect Synerex Server")
	}

	acClient = sxutil.NewSXServiceClient(client, pbase.AREA_COUNTER_SVC, "{Client:ACdbStore}")

	log.Print("Subscribe ACount Supply")
	acMu, acLoop = sxutil.SimpleSubscribeSupply(acClient, supplyACountCallback)

	wg.Add(1)
	wg.Wait()
}
