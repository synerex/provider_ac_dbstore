module ac_dbstore

go 1.15

require (
	github.com/go-sql-driver/mysql v1.6.0
	github.com/golang/protobuf v1.5.2
	github.com/synerex/proto_pcounter v0.0.6
	github.com/synerex/proto_storage v0.2.0
	github.com/synerex/synerex_api v0.5.1
	github.com/synerex/synerex_proto v0.1.12
	github.com/synerex/synerex_sxutil v0.7.0
)

replace github.com/synerex/proto_pcounter v0.0.6 => github.com/nagata-yoshiteru/proto_pcounter v0.0.10

replace github.com/synerex/synerex_proto v0.1.12 => github.com/nagata-yoshiteru/synerex_proto v0.1.16
