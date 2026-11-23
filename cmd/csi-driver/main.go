package main

import (
	"flag"
	"fmt"
	"os"

	driver "github.com/ngxstorage/fc-csi-ngxstorage-driver/pkg/driver"
	logutils "github.com/ngxstorage/go-logutils"
	zap "go.uber.org/zap"
)

func main() {
	var (
		endpoint      = flag.String("endpoint", "defaultEndpointValue", "gRPC endpoint")
		ip            = flag.String("ip", "defaultIPValue", "NGX Storage IP")
		poolName      = flag.String("pool-name", "defaultPoolNameValue", "Pool name")
		fcTargetNames = flag.String("fc-target-names", "", "Comma-separated pre-created FC target names")
		nodeId        = flag.String("nodeid", "defaultNodeIDValue", "Node ID")
		role          = flag.String("role", "node", "CSI role: node, controller, or all (test harness)")
		apiKey        = flag.String("api-key", "defaultApiKeyValue", "API key")
		log           = flag.String("log", "info", "Console log level: debug, info, warn, error, dpanic, panic, fatal")
		// driver-prefix optionally prepends a user label to the canonical CSI
		// driver name so a second driver instance can serve a distinct NGX
		// backend on the same cluster (for example 201.fc.csi.ngxstorage.com).
		// An empty value preserves the original fc.csi.ngxstorage.com identity.
		driverPrefix = flag.String("driver-prefix", "", "Optional prefix prepended to the CSI driver name (for example 201.fc.csi.ngxstorage.com); empty preserves fc.csi.ngxstorage.com")
	)
	flag.Parse()

	logName := *nodeId
	if *role == "controller" {
		logName = "controller"
	}
	logInstance, err := logutils.NewFromEnvironment(*log, logName, "fc", *driverPrefix)
	if err != nil {
		fmt.Fprintf(os.Stderr, "logger initialization failed: %v; continuing stdout-only\n", err)
		logInstance, err = logutils.New(logutils.Config{FilePrefix: "fc", ConsoleLevel: *log})
		if err != nil {
			fmt.Fprintf(os.Stderr, "stdout logger initialization failed: %v\n", err)
			os.Exit(1)
		}
	}
	defer logInstance.Close()
	logger := logInstance.Logger

	newDriver, err := driver.NewDriver(driver.InputParameters{
		Logger:        logger,
		Endpoint:      *endpoint,
		ApiKey:        *apiKey,
		Ip:            *ip,
		PoolName:      *poolName,
		FcTargetNames: *fcTargetNames,
		NodeId:        *nodeId,
		Role:          *role,
		Log:           *log,
		// A non-empty prefix changes the effective CSI driver name to
		// <prefix>.fc.csi.ngxstorage.com so multiple driver instances can
		// register distinct CSIDriver identities on one cluster.
		DriverPrefix: *driverPrefix,
	})
	if err != nil {
		logger.Error("main: driver creation failed", zap.Error(err))
		_ = logInstance.Close()
		// Initialization failure must not look like a successful container
		// exit. Kubernetes can then restart the workload and surface the
		// failure through the pod status instead of leaving it stopped.
		os.Exit(1)
	}

	if err := newDriver.Run(); err != nil {
		logger.Error("main: driver run failed", zap.Error(err))
		_ = logInstance.Close()
		// A stopped CSI listener is not a healthy driver process. Return a
		// non-zero status so the workload restart policy can recover it.
		os.Exit(1)
	}
}
