package state

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

func LoadState(fileName string, reset bool) (map[string]map[string]bool, error) {
	processed := make(map[string]map[string]bool)

	if reset {
		if err := os.Remove(fileName); err == nil {
			log.Printf("[INFO] Previous %s deleted due to --reset-state flag.\n", fileName)
			return processed, nil
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("could not delete %s: %w", fileName, err)
		}
	} else {
		stateFile, err := os.Open(fileName)
		if err == nil {
			defer stateFile.Close()
			var stateMap map[string][]string
			decoder := json.NewDecoder(stateFile)
			if err := decoder.Decode(&stateMap); err == nil {
				for key, versions := range stateMap {
					if processed[key] == nil {
						processed[key] = make(map[string]bool)
					}
					for _, v := range versions {
						processed[key][v] = true
					}
				}
			} else {
				log.Printf("[WARN] Failed to decode %s: %v\n", fileName, err)
			}
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("error opening %s: %w", fileName, err)
		}
	}

	return processed, nil
}

func SaveState(fileName string, processed map[string]map[string]bool) error {
	stateMap := make(map[string][]string)
	for key, versionsMap := range processed {
		for ver := range versionsMap {
			stateMap[key] = append(stateMap[key], ver)
		}
	}
	stateFileOut, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer stateFileOut.Close()

	encoder := json.NewEncoder(stateFileOut)
	encoder.SetIndent("", "  ")
	return encoder.Encode(stateMap)
}
