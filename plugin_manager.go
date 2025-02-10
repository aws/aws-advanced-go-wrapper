// TODO: change package back to driver...this was set to main so hello.go could run
package main

import "database/sql/driver"

type PluginManager interface {
	Init()
	Prepare(prepareFunc PrepareFunc) (driver.Stmt, error)
}

type ConnectionPluginManager struct {
	targetDriver driver.Driver
	plugins      []ConnectionPlugin
}

func (pluginManager *ConnectionPluginManager) Init() {
	// TODO: hardcoded...parameterize later
	pluginManager.plugins = []ConnectionPlugin{
		DummyPlugin{0},
		DummyPlugin{1},
		DummyPlugin{2}}
}

func (pluginManager *ConnectionPluginManager) Prepare(prepareFunc PrepareFunc) (driver.Stmt, error) {
	pluginChain := prepareFunc
	for _, plugin := range pluginManager.plugins {
		oldPluginChain := pluginChain
		pluginChain = func() (driver.Stmt, error) {
			return plugin.Prepare(oldPluginChain)
		}
	}
	return pluginChain()
}
