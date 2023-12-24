package main

import (
	// "bufio"
	"io"
	"bytes"
	"context"
	// "encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"github.com/gin-gonic/gin"
	// "golang.org/x/sync/syncmap"
)

// import _ "net/http/pprof"

// func init() {
// 	// http.HandleFunc("/debug/pprof/", Index)
// 	// http.HandleFunc("/debug/pprof/cmdline", Cmdline)
// 	// http.HandleFunc("/debug/pprof/profile", Profile)
// 	// http.HandleFunc("/debug/pprof/symbol", Symbol)
// 	// http.HandleFunc("/debug/pprof/trace", Trace)

// 	go func() {
// 		log.Println(http.ListenAndServe("127.0.0.1:6060", nil))
// 	}()
// }

type probeData struct {
	ID      string `json:"probeId"`
	EventId string `json:"eventId"`
	Data    string `json:"data"`
	Age     int64  `json:"eventReceivedTime"`
}

type SafeMap struct {
	mu   sync.Mutex
	data map[string]probeData
}

type SafeMap2 struct {
	mu   sync.Mutex
	data map[string]string
}

// var data = make(map[string]probeData)
var otherHosts = getOtherHosts()
var queue = map[string]string{
	otherHosts[0]: "",
	otherHosts[1]: "",
}


var safeMap = &SafeMap{
	data: make(map[string]probeData),
}

var requestMap = &SafeMap2{
	data: make(map[string]string),
}

func updateRequestMap(key string, value string) {
	requestMap.mu.Lock()
	defer requestMap.mu.Unlock()
	requestMap.data[key] = value
}

func getRequestMap(key string) (string, bool) {
	requestMap.mu.Lock()
	defer requestMap.mu.Unlock()
	value, ok := requestMap.data[key]

	if !ok {
		return "", false
	}
	return value, true
}

func updateLocalData(key string, value probeData) {
	safeMap.mu.Lock()
	defer safeMap.mu.Unlock()
	safeMap.data[key] = value
}

func getLocalData(key string) (probeData, bool) {
	// safeMap.mu.Lock()
	// defer safeMap.mu.Unlock()

	// Perform read operation on the map
	value, ok := safeMap.data[key]

	if !ok {
		var probe probeData
		return probe, false
	}
	return value, true
}


// Store this in global variable
func getHostname() string {
	hostname, _ := os.Hostname()
	return hostname
}

func getOtherHosts() []string {
	// return []string{"localhost", "localhost"}
	hosts := []string{"n1", "n2", "n3"}
	currentHost := getHostname()
	otherHosts := []string{}
	for _, s := range hosts {
		if s != currentHost {
			otherHosts = append(otherHosts, s)
		}
	}

	return otherHosts
}


// Pre generate and Store this
func generateRandomString(length int) string {
	charSet := "abcdefghijklmno"
	result := make([]byte, length)

	for i := range result {
		result[i] = charSet[rand.Intn(len(charSet))]
	}

	return string(result)
}



func sendUdpMessage(message []byte, host string) {
	serverAddr, err := net.ResolveUDPAddr("udp", host+":8080")
	if err != nil {
		fmt.Println("Error resolving UDP address:", err)
		return
	}
	// Create UDP connection
	conn, err := net.DialUDP("udp", nil, serverAddr)
	if err != nil {
		fmt.Println("Error connecting to server:", err)
		return
	}
	defer conn.Close()

	// Send a message to the server
	_, err = conn.Write(message)
	if err != nil {
		fmt.Println("Error sending message:", err)
		return
	}
}

func sendUdpMessageWithResponse(message []byte, host string) string {
	randStr := generateRandomString(10)

	serverAddr, err := net.ResolveUDPAddr("udp", host+":8080")
	if err != nil {
		fmt.Println("Error resolving UDP address:", err)
		return ""
	}
	// Create UDP connection
	conn, err := net.DialUDP("udp", nil, serverAddr)
	if err != nil {
		fmt.Println("Error connecting to server:", err)
		return ""
	}
	defer conn.Close()

	toAppend := "," + randStr
	message = append(message, toAppend...)

	// Send a message to the server
	_, err = conn.Write(message)
	if err != nil {
		fmt.Println("Error sending message:", err)
		return ""
	}

	updateRequestMap(randStr, "")

	val := ""
	for i := 0; i < 10; i++ {
		val, _ = getRequestMap(randStr)
		if i == 9 {
			val = "nil"
			break
		}
		time.Sleep(time.Microsecond)
	}

	if val == "nil" {
		return ""
	}
	fmt.Println("response for " + string(message) + "->" + val)
	return val

}

// func fetchProbeFromNode(n string, probeId string) (probeData, bool) {
// 	payload := []byte(fmt.Sprintf(`%s,%s,%s`, probeId, "get", getHostname()))

// 	var probe probeData
// 	fetchedData := sendUdpMessageWithResponse(payload, n)
// 	fmt.Println("fetchedData", fetchedData)
// 	if fetchedData == "" {
// 		return probe, false
// 	}

// 	splitMessage := strings.Split(fetchedData, ",")
// 	ageInt, _ := strconv.ParseInt(splitMessage[2], 10, 64)
// 	newProbe := probeData{
// 		ID:      splitMessage[3],
// 		EventId: splitMessage[0],
// 		Data:    splitMessage[1],
// 		Age:     ageInt,
// 	}

// 	return newProbe, true
// }

func fetchProbeFromNode(n string, probeId string) (probeData, bool) {
	var probe probeData
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	client := &http.Client{}

	req, _ := http.NewRequest("GET", fmt.Sprintf("http://%s:9000/internal/probe/%s", n, probeId), nil)
	req = req.WithContext(ctx)

	resp, e := client.Do(req)
	if e != nil {
		fmt.Println(e)
		return probe, false
	}
    respBody := resp.Body
    body, _ := io.ReadAll(respBody)
	bodyStr := string(body)
    fmt.Println(n, "bodyStr", bodyStr)
	split := strings.Split(bodyStr, ",")
	for i,s := range split {
		fmt.Println("split value", i, s)
		if s == "" {
			return probe, false
		}
	}
	ageInt, _ := strconv.ParseInt(split[3], 10, 64)
	newProbe := probeData{
		ID:      split[0],
		EventId: split[1],
		Data:    split[2],
		Age:     ageInt,
	}
	fmt.Println("new probe created")
	fmt.Println(newProbe)
	return newProbe, true
	// body2, _ := io.ReadAll(resp.Body)
	// var result map[string]interface{}

	// err := json.Unmarshal(body2, &result)
	// fmt.Println("result")
	// fmt.Println(result)


	// fmt.Println("fetch probe", probe.EventId, probe.Data)
	// if err != nil || probe.EventId == ""{
	// 	fmt.Println("Error fetch probe", err)
	// 	return probe, false
	// }

	// return probe, true
}

func findSimilar(first, second, third string) int {
    // Check if any two strings are equal (ignoring empty strings)
    if first != "" && (first == second || first == third) {
        return 1
    }

    if second != "" && second == third {
        return 2
    }

	return 10
}


func getConsensusFor(probeId string) (probeData, bool) {
	var probe probeData
	own, ok := getLocalData(probeId)
	resp1, ok1 := fetchProbeFromNode(otherHosts[0], probeId)
	resp2, ok2 := fetchProbeFromNode(otherHosts[1], probeId)

	var considerations []string
	if ok {
		considerations = append(considerations, own.EventId)
	} else {
		considerations = append(considerations, "")
	}
	if ok1 {
		considerations = append(considerations, resp1.EventId)
	} else {
		considerations = append(considerations, "")
	}

	if ok2 {
		considerations = append(considerations, resp2.EventId)
	} else {
		considerations = append(considerations, "")
	}
	fmt.Println("Considerations", probeId, considerations)

	num := findSimilar(considerations[0], considerations[1], considerations[2])
	if num == 1 {
		return own, true
	}
	if num == 2 {
		return resp1, true
	}
	return probe, false
}
// func getConsensusFor(probeId string) (probeData, bool) {
// 	var probe probeData
// 	own, ok := getLocalData(probeId)
// 	resp1, ok1 := fetchProbeFromNode(otherHosts[0], probeId)

// 	if ok1 && ok {
// 		if resp1.Data == own.Data {
// 			return resp1, true
// 		}
// 	}

// 	resp2, ok2 := fetchProbeFromNode(otherHosts[1], probeId)

// 	if ok {
// 		if ok1 {
// 			if resp1.Age > own.Age {
// 				return resp1, true
// 			}
// 		}
// 		if ok2 {
// 			if resp2 == own {
// 				return resp2, true
// 			} else {
// 				if resp2.Age > own.Age {
// 					return resp2, true
// 				} else {
// 					return own, true
// 				}

// 			}
// 		}
// 		if !ok1 && !ok2 {
// 		return own, true
// 	}
// 	} else {
// 		if ok1 {
// 			if ok2 {
// 				if resp2 == resp1 {
// 					return resp2, true
// 				} else {
// 					if resp2.Age > resp1.Age {
// 						return resp2, true
// 					} else {
// 						return resp1, true
// 					}

// 				}
// 			} else {
// 				return resp1, true
// 			}
// 		} else {
// 			if ok2 {
// 				return resp2, true
// 			}
// 		}
// 	}
// 	return probe, false
// }

// func sendUpdateToOneOtherHost(probeId string, eventId string, data string, age int64, updateStr string) {
// 	payload := []byte(fmt.Sprintf(`%s,%s,%s,%s,%s`, eventId, data, strconv.FormatInt(age, 10), probeId, "put"))
// 	for i, host := range getOtherHosts() {
// 		if i == 0 {
// 			sendUdpMessage(payload, host)
// 		} else {
// 			// continue
// 			go sendUdpMessage(payload, host)
// 		}
// 	}
// }

func sendUpdateToOneOtherHost(probeId string, eventId string, data string, age int64, updateStr string) bool {
	for _, host := range getOtherHosts() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		client := &http.Client{}
		url := fmt.Sprintf("http://%s:9000/internal/probe/%s", host, probeId)
		payload := []byte(fmt.Sprintf(`{"eventId":"%s","data":"%s","age":"%s"}`, eventId, data, strconv.FormatInt(age, 10)))

		request, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(payload))
		if err != nil {
			fmt.Println("Error creating request:", err)
			continue
		}

		request.Header.Set("Content-Type", "application/json")
		request = request.WithContext(ctx)

		response, err := client.Do(request)
		if err == nil {
			return true
		}
		if err != nil {
			fmt.Println("Error while putting", host, err)
			continue
		}
		defer response.Body.Close()
	}
	return false
}


func runHttpServer() {
	router := gin.Default()
	router.GET("/probe/:id", getProbeById)
	router.PUT("/probe/:id", putProbe)
	router.PUT("/internal/probe/:id", updateDataFromOtherHost)
	router.GET("/internal/probe/:id", getInternalProbe)
	router.GET("/internal/queue/:nodeId", getQueueForNode)
	fmt.Println("Server starting...")
	err := router.Run("0.0.0.0:9000")
	if err != nil {
		fmt.Println(err)
	}
}

func processUdpInbound(message string) {
	fmt.Println("Message received", message)
	splitMessage := strings.Split(message, ",")

	if len(splitMessage) == 5 && splitMessage[4] == "put" {
		eventId := splitMessage[0]
		data := splitMessage[1]
		age := splitMessage[2]
		probeId := splitMessage[3]

		ageInt, _ := strconv.ParseInt(age, 10, 64)
		newProbe := probeData{
			ID:      probeId,
			EventId: eventId,
			Data:    data,
			Age:     ageInt,
		}

		updateLocalData(probeId, newProbe)
	}
	if len(splitMessage) == 4 && splitMessage[1] == "get" {
		probeId := splitMessage[0]
		host := splitMessage[2]
		probe, ok := getLocalData(probeId)
		var data []byte
		if ok {
			data = []byte(fmt.Sprintf(`%s,%s,%s,%s,%s,%s`, probe.EventId, probe.Data, strconv.FormatInt(probe.Age, 10), probeId, "response", splitMessage[3]))
		} else {
			data = []byte(fmt.Sprintf(`%s,%s,%s,%s,%s,%s`, "", "", "", "", "response", splitMessage[3]))
		}

		sendUdpMessage(data, host)
	}
	if len(splitMessage) == 6 && splitMessage[4] == "response" {
		if splitMessage[0] == "" {
			updateRequestMap(splitMessage[5], "nil")
			fmt.Println("randStr", splitMessage[5])

		} else {
			updateRequestMap(splitMessage[5], fmt.Sprintf(`%s,%s,%s,%s`, splitMessage[0], splitMessage[1], splitMessage[2], splitMessage[3]))
		}
	}

}

func runUdpServer() {
	// Listen on UDP port 8080
	serverAddr, err := net.ResolveUDPAddr("udp", ":8080")
	if err != nil {
		fmt.Println("Error resolving UDP address:", err)
		return
	}

	conn, err := net.ListenUDP("udp", serverAddr)
	if err != nil {
		fmt.Println("Error listening:", err)
		return
	}
	defer conn.Close()

	fmt.Println("UDP Server listening on", serverAddr)

	buffer := make([]byte, 2048)

	for {
		// Read data from the client
		n, clientAddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Println("Error reading:", err)
			continue
		}

		// Print the received message
		processUdpInbound(string(buffer[:n]))
		fmt.Println(clientAddr)

		// Send a response back to the client
		// conn.WriteToUDP([]byte("Message received"), clientAddr)
	}
}
// var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func main() {
	// flag.Parse()
    // if *cpuprofile != "" {
    //     f, err := os.Create(*cpuprofile)
    //     if err != nil {
    //         log.Fatal(err)
    //     }
    //     pprof.StartCPUProfile(f)
    //     defer pprof.StopCPUProfile()
    // }
	// fmt.Println("hello")
	// time.Sleep(2*time.Second)
	var wg sync.WaitGroup
	wg.Add(1)
	go runHttpServer()
	// go runUdpServer()
	wg.Wait()

}

func putProbe(c *gin.Context) {
	id := c.Param("id")
	var requestBody struct {
		EventId string `json:"eventId"`
		Data    string `json:"data"`
	}

	if err := c.BindJSON(&requestBody); err != nil {
		return
	}

	age := time.Now().UnixNano() / int64(time.Millisecond)
	newProbe := probeData{
		ID:      id,
		EventId: requestBody.EventId,
		Data:    requestBody.Data,
		Age:     age,
	}

	newStr := fmt.Sprintf("probeId:%s,eventId:%s,data:%s,age:%d;", newProbe.ID, newProbe.EventId, newProbe.Data, newProbe.Age)
	updated := sendUpdateToOneOtherHost(id, newProbe.EventId, newProbe.Data, age, newStr)
	if !updated {
	c.Status(503)
	return
}
	updateLocalData(id, newProbe)
	fmt.Println(newStr)

	c.JSON(http.StatusOK, newProbe)
}

func getProbeById(c *gin.Context) {
	// c.JSON(http.StatusNotFound, gin.H{"message": "probe not found"})
	// return

	id := c.Param("id")
	fmt.Println("Get query Received for", id)

	probe, ok := getConsensusFor(id)
	fmt.Println(getHostname(), "Got consensesus for", id, probe.EventId)
	updateLocalData(id, probe)
	if ok {
		c.JSON(http.StatusOK, probe)
		return
	}

	c.JSON(http.StatusNotFound, gin.H{"message": "probe not found"})
}

func getInternalProbe(c *gin.Context) {
	probeId := c.Param("id")
	a, ok := getLocalData(probeId)

	if ok {
		age := strconv.FormatInt(a.Age, 10)
		newStr := fmt.Sprintf("%s,%s,%s,%s;", a.ID, a.EventId, a.Data, age)
		fmt.Println("internal find", a.EventId)
		c.String(http.StatusOK, newStr)
		return
	}

	c.String(http.StatusBadRequest, "")
}

func getQueueForNode(c *gin.Context) {
	nodeId := c.Param("nodeId")
	q, ok := queue[nodeId]
	if ok {
		// queue[nodeId] = ""
		c.Data(http.StatusOK, "application/json; charset=utf-8", []byte(q))
		return
	}
	c.JSON(http.StatusNotFound, gin.H{"message": "probe not found"})
}

func updateDataFromOtherHost(c *gin.Context) {
	id := c.Param("id")

	var requestBody struct {
		EventId string `json:"eventId"`
		Data    string `json:"data"`
		Age     string `json:"age"`
	}
	if err := c.BindJSON(&requestBody); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("request body from other host", requestBody)
	c.Status(http.StatusOK)
	ageInt, _ := strconv.ParseInt(requestBody.Age, 10, 64)
	newProbe := probeData{
		ID:      id,
		EventId: requestBody.EventId,
		Data:    requestBody.Data,
		Age:     ageInt,
	}

	// fmt.Printf("%s - %d - %s - %s\n", id, newProbe.Age, newProbe.EventId, requestBody.Data)

	updateLocalData(id, newProbe)
	fmt.Printf("Time: %d - Host: %s - Probe: %s - Event: %s\n", time.Now().UnixNano()/int64(time.Millisecond), getHostname(), id, requestBody.EventId)
}
