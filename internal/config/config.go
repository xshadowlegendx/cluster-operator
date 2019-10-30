package config

import (
	"fmt"

	"gopkg.in/yaml.v2"
)

type serviceConfig struct {
	Type        string            `yaml:"type"`
	Annotations map[string]string `yaml:"annotations"`
}

type persistenceConfig struct {
	StorageClassName string `yaml:"storageClassName"`
	Storage          string `yaml:"storage"`
}

type resourcesConfig struct {
	Limits   limitsConfig   `yaml:"limits"`
	Requests requestsConfig `yaml:"requests"`
}

type limitsConfig struct {
	CPU    string `yaml:"cpu"`
	Memory string `yaml:"memory"`
}

type requestsConfig struct {
	CPU    string `yaml:"cpu"`
	Memory string `yaml:"memory"`
}

type Config struct {
	Service         serviceConfig     `yaml:"service"`
	Persistence     persistenceConfig `yaml:"persistence"`
	ImagePullSecret string            `yaml:"imagePullSecret"`
	Image           string            `yaml:"image"`
	Resources       resourcesConfig   `yaml:"resources"`
}

func NewConfig(configRaw []byte) (*Config, error) {
	config := &Config{}
	var err error

	err = yaml.Unmarshal(configRaw, config)

	if err != nil {
		return nil, fmt.Errorf("could not unmarshal config: %s", err)
	}

	return config, err
}
