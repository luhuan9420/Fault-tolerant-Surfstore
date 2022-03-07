package surfstore

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// check if base dir is valid
	baseDir := client.BaseDir
	// log.Printf("base dir: %v\n", baseDir)
	localFiles, err := ioutil.ReadDir(baseDir)
	if err != nil {
		fmt.Errorf("Error when trying to read client base directory: %v\n", err)
	}

	// check index.txt file
	indexPath := client.BaseDir + "/index.txt"
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		// log.Print("Index file not exist\n")
		file, err := os.Create(indexPath)
		if err != nil {
			log.Fatalf("Fail to create index file: %v\n", err)
		}
		defer file.Close()
	}

	// get file info map with local files
	localFileInfoMap, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Fatalf("Error when trying to load local file info map: %v\n", err)
	}

	fileDelete := make(map[string]bool)
	for filename, _ := range localFileInfoMap {
		fileDelete[filename] = false
	}
	/* compare dir files with index.txt
	1. check if there is new files in dir that are not in index.txt
	2. check if any file in index.txt hash list is different (have changed since last time client sync)
	*/
	// fileModified := make(map[string]string)
	fileModified := make(map[string]bool)
	fileNew := make(map[string]bool)
	// fileNew := make(map[string]string)
	for _, f := range localFiles {
		log.Printf("file name: %v\n", f.Name())
		if f.Name() == "index.txt" {
			continue
		}
		file, err := os.Open(client.BaseDir + "/" + f.Name())
		if err != nil {
			log.Fatalf("Fail to open file %v: %v\n", f.Name(), err)
		}
		fileSize := f.Size()
		blockNeeded := int(float64(fileSize)/float64(client.BlockSize)) + 1
		log.Printf("Block needed: %v\n", blockNeeded)

		// file in index.txt, check if hash list is different
		if fmd, ok := localFileInfoMap[f.Name()]; ok {
			log.Printf("fmd hash list size: %v", len(fmd.GetBlockHashList()))
			delete(fileDelete, f.Name())
			thisHashList := make([]string, blockNeeded)
			changed := false
			for i := 0; i < blockNeeded; i++ {
				buf := make([]byte, client.BlockSize)
				size, err := file.Read(buf)
				if err != nil {
					// log.Printf("error when trying to get hash list: %v\n", err)
					log.Fatalf("error when trying to get hash list: %v\n", err)
				}
				buf = buf[:size]

				hashString := GetBlockHashString(buf)
				thisHashList[i] = hashString
				log.Printf("hash string: %v", hashString)
				// log.Printf("fmd hash list: %v", fmd.GetBlockHashList()[i])
				if i >= len(fmd.GetBlockHashList()) || hashString != fmd.GetBlockHashList()[i] {
					changed = true
				}
			}
			if blockNeeded != len(fmd.GetBlockHashList()) {
				changed = true
			}
			// hashStr := GetHashString(thisHashList)
			if changed {
				localFileInfoMap[f.Name()].BlockHashList = thisHashList
				//fileModified[f.Name()] = f.Name() + "," + strconv.Itoa(fmd.GetVersion()) + "," + hashStr
				fileModified[f.Name()] = true
				log.Printf("%v changed", f.Name())
			}
		} else {
			// new file
			// file in dir not in index.txt
			thisHashList := make([]string, blockNeeded)
			for i := 0; i < blockNeeded; i++ {
				buf := make([]byte, client.BlockSize)
				size, err := file.Read(buf)
				if err != nil {
					// log.Printf("error when trying to get hash list: %v\n", err)
					log.Fatalf("error when trying to get hash list: %v\n", err)
				}
				buf = buf[:size]

				hashString := GetBlockHashString(buf)
				thisHashList[i] = hashString
			}
			// hashStr := GetHashString(thisHashList)
			// fileNew[f.Name()] = f.Name() + "," + strconv.Itoa(1) + "," + hashStr
			fmd := &FileMetaData{
				Filename:      f.Name(),
				Version:       1,
				BlockHashList: thisHashList,
			}
			localFileInfoMap[f.Name()] = fmd
			fileNew[f.Name()] = true
		}
	}
	log.Printf("size of local file info map: %v\n", len(localFileInfoMap))
	// remaining key in fileDelete is the file that is deleted by client
	for filename, _ := range fileDelete {
		// deleted file
		fmd := &FileMetaData{
			Filename:      filename,
			Version:       localFileInfoMap[filename].GetVersion(),
			BlockHashList: []string{"0"},
		}
		localFileInfoMap[filename] = fmd
		fileModified[filename] = true
		log.Printf("Version after delete: %v", fmd.GetVersion())
	}

	// connect to server
	// download updated FileInfoMap
	serverFileInfoMap := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&serverFileInfoMap)
	if err != nil {
		// fmt.Errorf("Error when trying to get file info from server: %v\n", err)
		log.Fatalf("Error when trying to get file info from server: %v\n", err)
	}
	log.Printf("size of server file info map: %v\n", len(serverFileInfoMap))
	fmt.Print("server file info map")
	PrintMetaMap(serverFileInfoMap)

	/* compare local index to remote index
	1. remote index refers to a file not present in local index or base dir
	1.1 download blocks associated with that file
	1.2 reconstitute file in base dir
	1.3 add updated FileInfo to local index.txt

	2. new file in local base dir not in local index.txt nor remote index.txt
	2.1 upload blocks corresponding to this file to server
	2.2 update server with new FileInfo
	2.3 update local index if success
	*** might need to handle version conflict ***
	*/
	for fileName, fmd := range localFileInfoMap {
		if serverMD, ok := serverFileInfoMap[fileName]; ok {
			log.Printf("local version: %v", fmd.GetVersion())
			log.Printf("server version: %v", serverMD.GetVersion())
			modified := fileModified[fileName]
			log.Printf("Modified file? %v", modified)
			// if dataA and dataB both have one file with different content, they have the same version, but the modified is false
			// what to do to pass this test case? update sesrver
			// check if local file hash list is different than server file
			new := fileNew[fileName]
			log.Printf("New file? %v", new)

			if fmd.GetVersion() == serverMD.GetVersion() && !modified && !new {
				continue
			} else if fmd.GetVersion() == serverMD.GetVersion() && new {
				cleintSideUpdate(client, serverMD, &localFileInfoMap)
			} else if (fmd.GetVersion() > serverMD.GetVersion()) || (fmd.GetVersion() == serverMD.GetVersion() && modified) {
				// server side file is old (or client side file is updated)
				serverSideUpdate(client, fmd, modified, &localFileInfoMap)
			} else {
				// client side file is old
				cleintSideUpdate(client, serverMD, &localFileInfoMap)
			}
		} else {
			uploadNew(client, fmd, &localFileInfoMap)
		}
	}
	server2 := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&server2)
	log.Printf("size of server file info map: %v\n", len(server2))
	// for fileName, fmd := range fileNew {
	// 	if serverMD, ok := serverFileInfoMap[fileName]; ok {
	// 		clientSideUpdate(client, fmd, false, )
	// 	} else {
	// 		uploadNew(client, fmd)
	// 	}
	// }

	// download new files from server
	for filename, serverMD := range serverFileInfoMap {
		if _, exist := localFileInfoMap[filename]; !exist {
			localMD, err := download(client, filename, serverMD)
			if err != nil {
				// log.Printf("Failed to download file from server: %v\n", err)
				log.Fatalf("Failed to download file from server: %v\n", err)
			}
			localFileInfoMap[filename] = localMD
		}
	}
	fmt.Println("local file info map: ")
	PrintMetaMap(localFileInfoMap)

	err = WriteMetaFile(localFileInfoMap, client.BaseDir)
	// if err != nil {
	// 	log.Fatal("Fail to update index.txt: %v\n", err)
	// }

	// handle conflict
}

func GetHashString(hashList []string) string {
	hashStr := ""
	for i, hash := range hashList {
		hashStr += hash
		if i != (len(hashList) - 1) {
			hashStr += " "
		}
	}
	return hashStr
}

func serverSideUpdate(client RPCClient, clientMD *FileMetaData, modified bool, localFileInfoMap *map[string]*FileMetaData) {
	log.Printf("Update server...")
	// if client file has been updated, version needs to be udpated
	if modified {
		clientMD.Version += 1
		(*localFileInfoMap)[clientMD.GetFilename()] = clientMD
	}
	err := uploadNew(client, clientMD, localFileInfoMap)
	if err != nil {
		// log.Printf("Fail to upload file: %v\n", err)
		log.Fatalf("Fail to upload file: %v\n", err)
	}
}

func cleintSideUpdate(client RPCClient, serverMD *FileMetaData, localFileInfoMap *map[string]*FileMetaData) {
	log.Printf("Update client...")
	downloadMD, err := download(client, serverMD.GetFilename(), serverMD)

	if err != nil {
		// log.Printf("Fail to download file from server: %v\n", err)
		log.Fatalf("Fail to download file from server: %v\n", err)
	}
	(*localFileInfoMap)[serverMD.GetFilename()] = downloadMD
}

func uploadNew(client RPCClient, fmd *FileMetaData, localFileInfoMap *map[string]*FileMetaData) error {
	log.Println("Start uploading...")
	// log.Printf("File name: %v\n", fmd.GetFilename())
	filePath := client.BaseDir + "/" + fmd.GetFilename()
	// log.Printf("File path: %v\n", filePath)

	if _, e := os.Stat(filePath); os.IsNotExist(e) {
		var version int32
		err := client.UpdateFile(fmd, &version)
		fmt.Printf("Version: %v\n", version)
		fmt.Printf("FMD version: %v\n", fmd.GetVersion())
		if err != nil {
			log.Printf("Failed to update file: %v\n", err)
			// log.Fatalf("Failed to update file: %v\n", err)
		}
		return err
	}

	file, err := os.Open(filePath)
	if err != nil {
		// log.Printf("Fail to open file: %v\n", err)
		log.Fatalf("Fail to open file: %v\n", err)
	}
	defer file.Close()

	f, _ := os.Stat(filePath)
	blockNeeded := int(float64(f.Size())/float64(client.BlockSize)) + 1
	// fmt.Printf("block needed: %v\n", blockNeeded)
	log.Printf("block needed: %v\n", blockNeeded)

	for i := 0; i < blockNeeded; i++ {
		var block Block
		block.BlockData = make([]byte, client.BlockSize)
		n, err := file.Read(block.BlockData)
		if err != nil && err != io.EOF {
			// log.Printf("Fail to read file: %v\n", err)
			log.Fatalf("Fail to read file: %v\n", err)
		}
		block.BlockSize = int32(n)
		block.BlockData = block.BlockData[:n]

		var blockStoreAddr string
		err = client.GetBlockStoreAddr(&blockStoreAddr)
		if err != nil {
			// log.Printf("Fail to get block store address: %v\n", err)
			log.Fatalf("Fail to get block store address: %v\n", err)
		}
		// log.Printf("block store addr: %v\n", blockStoreAddr)
		var succ bool
		err = client.PutBlock(&block, blockStoreAddr, &succ)
		// log.Printf("Put block success? %v\n", succ)
		if err != nil {
			// log.Printf("Fail to put block: %v\n", err)
			log.Fatalf("Fail to put block: %v\n", err)
		}

	}

	var version int32
	err = client.UpdateFile(fmd, &version)
	fmt.Printf("Version: %v\n", version)
	fmt.Printf("FMD version: %v\n", fmd.GetVersion())

	if err != nil {
		log.Printf("Failed to update file: %v\n", err)
		return err
	}
	if version == -1 {
		// version mismatch
		log.Println("Version mismatch")
		// log.Fatalf("Failed to update file: %v\n", err)
		// download newest files from server
		serverFileInfoMap := make(map[string]*FileMetaData)
		err = client.GetFileInfoMap(&serverFileInfoMap)
		if err != nil {
			// log.Printf("Fail to get file info: %v\n", err)
			log.Fatalf("Fail to get file info: %v\n", err)
		}
		cleintSideUpdate(client, serverFileInfoMap[fmd.GetFilename()], localFileInfoMap)
	}
	log.Println("Finish uploading...")
	return err
}

func download(client RPCClient, filename string, serverMD *FileMetaData) (*FileMetaData, error) {
	log.Println("Start downloading...")
	filePath := client.BaseDir + "/" + filename

	var file *os.File
	var err error
	if _, e := os.Stat(filePath); os.IsNotExist(e) {
		file, err = os.Create(filePath)
		if err != nil {
			log.Fatalf("Fail to create file: %v\n", err)
		}
	} else {
		err = os.Remove(filePath)
		if err != nil {
			log.Fatalf("Fail to delete file: %v\n", err)
		}
		file, err = os.Create(filePath)
		if err != nil {
			log.Fatalf("Fail to create file: %v\n", err)
		}
	}

	// file is deleted in server
	if len(serverMD.GetBlockHashList()) == 1 && serverMD.GetBlockHashList()[0] == "0" {
		err := os.Remove(filePath)
		if err != nil {
			log.Printf("Fail to delete file: %v\n", err)
			// log.Fatalf("Fail to delete file: %v\n", err)
		}
		return serverMD, err
	}

	var blockStoreAddr string
	err = client.GetBlockStoreAddr(&blockStoreAddr)
	if err != nil {
		// log.Printf("Fail to get block store address: %v\n", err)
		log.Fatalf("Fail to get block store address: %v\n", err)
	}

	for i, hash := range serverMD.GetBlockHashList() {
		// fmt.Printf("hash list %v\n", i)
		log.Printf("hash list %v\n", i)
		var block Block
		err := client.GetBlock(hash, blockStoreAddr, &block)
		log.Printf("Block size: %v\n", block.GetBlockSize())
		if err != nil {
			// log.Printf("Fail to get block: %v\n", err)
			log.Fatalf("Fail to get block: %v\n", err)
		}

		_, err = file.Write(block.BlockData)
		if err != nil {
			log.Fatalf("Fail to write block data: %v\n", err)
		}
	}
	fmd := &FileMetaData{
		Filename:      serverMD.GetFilename(),
		Version:       serverMD.GetVersion(),
		BlockHashList: serverMD.GetBlockHashList(),
	}
	log.Println("Finish downloading...")
	return fmd, err
}

func hashListNotEqual(server, local []string) bool {
	if len(server) != len(local) {
		return true
	}
	for i, v := range server {
		if v != local[i] {
			return true
		}
	}
	return false
}
