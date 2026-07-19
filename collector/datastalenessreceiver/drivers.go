package datastalenessreceiver

// Register pure-Go SQL drivers so the "sql" scraper works out of the box for
// the two most common databases without a custom build. Both are cgo-free,
// keeping the Collector binary statically linkable.
//
// To support another database, add a blank import of its driver in your
// Collector build and reference it via the source's `driver` field.
import (
	_ "github.com/go-sql-driver/mysql" // driver name: "mysql"
	_ "github.com/lib/pq"              // driver name: "postgres"
)
