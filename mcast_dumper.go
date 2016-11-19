package main

/**
 * @author Ivan Dudko <ivan.dudko@gmail.com>
 */

import (
	"fmt"
	"time"
	"net"
	"log"
	"strconv"
	"os"
	"encoding/json"
	"golang.org/x/net/ipv4"
	"syscall"
	"errors"
	"container/ring"
	"sort"
	"flag"
)

const (
	MTU = 1500
	UDP_READ_TIMEOUT = 3
	STREAM_BITRATE_CHECK_INTERVAL = 5
	STREAM_BITRATE_RECOVERY_INTERVAL = 10
	STREAM_BITRATE_MIN_THRESHOLD_VALUE = 10000
	APP_VERSION = "0.0.1"
)

type Source struct {
	Id int
	Address string
	Interface string
	Priority int
	AverageBitrate int
	Alive bool
	LastStatusChange time.Time
	QuitSignal chan bool
	StreamData chan []byte
}

type Configuration struct {
	Sources []Source
}

func (stream *Source) setSourceAlive(status bool) {
	if !status {
		stream.AverageBitrate = 0
	}
	stream.Alive = status
	stream.LastStatusChange = time.Now()
}

type ByPriority []Source

func (a ByPriority) Len() int {
	return len(a)
}

func (a ByPriority) Swap(i, j int){
	a[i], a[j] = a[j], a[i]
}

func (a ByPriority) Less(i, j int) bool{
	return a[i].Priority < a[j].Priority
}

func getMulticastAddressFromString(addr string) net.UDPAddr{
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		log.Println("Can't split UDP source address and port", err)
	}
	int_port, err := strconv.ParseInt(port, 10, 0)
	if err != nil {
		log.Println("Bad UDP source port:", err)
	}
	group := net.UDPAddr{IP: net.ParseIP(host), Port: int(int_port)}
	log.Print(group)
	return group
}

func loadConfiguration(config string) (c Configuration){
	file, _ := os.Open(config)
	decoder := json.NewDecoder(file)
	configuration := Configuration{}
	err := decoder.Decode(&configuration)
	if err != nil {
		log.Println("Error while loading configuration file:", err)
	}
	return configuration
}

// Returns UDP Multicast packet connection to read incoming bytes from
func getStreamConnection(src *Source) (net.PacketConn, error) {
	group := getMulticastAddressFromString(src.Address)

	socketFile, err := getSocketFile(group)
	if err != nil {
		return nil, err
	}
	conn, err := net.FilePacketConn(socketFile)
	if err != nil {
		log.Printf("Failed to get packet file connection: %s", err)
		return nil, err
	}
	socketFile.Close()

	netInterface, _ := net.InterfaceByName(src.Interface)
	if err := ipv4.NewPacketConn(conn).JoinGroup(netInterface, &group); err != nil {
		log.Printf("Failed to join mulitcast group: %s", err)
		return nil, err
	}
	return conn, nil
}

// Returns bound UDP socket
func getSocketFile(group net.UDPAddr) (*os.File, error) {
	addr := group.IP.To4()

	sock, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, 0)
	if err != nil {
		log.Printf("Syscall.Socket: %s", err)
		return nil, errors.New("Can't create socket")
	}

	syscall.SetsockoptInt(sock, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	lsa := &syscall.SockaddrInet4{Port: group.Port, Addr: [4]byte{addr[0], addr[1], addr[2], addr[3]}}

	if err := syscall.Bind(sock, lsa); err != nil {
		log.Printf("Syscall.Bind: %s", err)
		return nil, errors.New("Can't bind socket")
	}

	return os.NewFile(uintptr(sock), "udp4:" + group.IP.String() + ":" + strconv.Itoa(group.Port) + "->"), nil
}

func getMulticastData(src *Source) {

	lastData := ring.New(STREAM_BITRATE_CHECK_INTERVAL)
	buf := make([]byte, MTU)
	var sumBitrate, packetSizeSum int

	conn, err := getStreamConnection(src)
	if err != nil {
		return
	}
	defer conn.Close()

	localAddress := conn.LocalAddr().String()

	ticker := time.NewTicker(time.Second)

	if src.Address == localAddress {
		for {
			select {
			case <-ticker.C:
				src.AverageBitrate = 0
				lastData = lastData.Next()
				lastData.Value = packetSizeSum * 8
				lastData.Do(func(element interface{}) {
					if element != nil {
						sumBitrate += element.(int)
					}
				})
				src.AverageBitrate = sumBitrate / STREAM_BITRATE_CHECK_INTERVAL
				if src.AverageBitrate < STREAM_BITRATE_MIN_THRESHOLD_VALUE && src.Alive {
					src.setSourceAlive(false)
					lastData.Do(func(element interface{}) {
						if element != nil {
							element = 0
						}
					})
				}
				if  (time.Now().Sub(src.LastStatusChange) > time.Duration(STREAM_BITRATE_RECOVERY_INTERVAL)*time.Second) && !src.Alive && src.AverageBitrate > STREAM_BITRATE_MIN_THRESHOLD_VALUE {
					src.setSourceAlive(true)
				}
				packetSizeSum = 0
				sumBitrate = 0
			case <-src.QuitSignal:
				log.Println("Recieved QUIT signal for stream =", src.Id)
				conn.Close()
				return
			default:
			}

			conn.SetReadDeadline(time.Now().Add(time.Second * UDP_READ_TIMEOUT))

			packetSize, _, err := conn.ReadFrom(buf)
			if err != nil {
				src.setSourceAlive(false)
				log.Println("Read data from multicast stream error:", err)
			}
			packetSizeSum += packetSize

			select {
			case src.StreamData <- buf[:packetSize]:

			default:
			}
			/*if packetSize > 0 {
				src.StreamData <- buf[:packetSize]
			} else {
				src.StreamData <- 0
			}*/
			//src.StreamData <- buf[:packetSize]

			//fmt.Println(len(buf))

			/*if _, err := os.Stdout.Write(buf[:packetSize]); err != nil {
                		log.Fatal(err)

                	}*/
		}
	}
}

func main(){
	version := flag.Bool("version", false, "prints application version")
	flag.Parse()
	if *version {
		fmt.Println(APP_VERSION)
		os.Exit(0)
	}

	ticker := time.NewTicker(time.Second)
	configuration := loadConfiguration("config.json")
	sort.Sort(sort.Reverse(ByPriority(configuration.Sources)))
	for n:=0; n < len(configuration.Sources); n++ {
		configuration.Sources[n].Id = n
		configuration.Sources[n].Alive = true
		configuration.Sources[n].AverageBitrate = 0
		configuration.Sources[n].LastStatusChange = time.Now()
		configuration.Sources[n].QuitSignal = make(chan bool)
		configuration.Sources[n].StreamData = make(chan []byte)
	}

	currentSourceId := 0
	go getMulticastData(&configuration.Sources[currentSourceId])

	for {
		select {
		case <- ticker.C:
			log.Println("Stream", currentSourceId, "average bitrate for", STREAM_BITRATE_CHECK_INTERVAL, "seconds is", configuration.Sources[currentSourceId].AverageBitrate, "bps")

			for i := 0; i < len(configuration.Sources); i++ {
				if !configuration.Sources[currentSourceId].Alive {
					if configuration.Sources[i].Alive {
						log.Println("Search for alive stream. Stream ", i,"is maybe available!")
						currentSourceId = i
						go getMulticastData(&configuration.Sources[i])
					}
				}

				if configuration.Sources[i].Alive && configuration.Sources[currentSourceId].Alive && configuration.Sources[currentSourceId].AverageBitrate > STREAM_BITRATE_MIN_THRESHOLD_VALUE {
					if currentSourceId > i {
						log.Println("Stopping old goroutines..")
						fmt.Println(<-configuration.Sources[0].StreamData)
						configuration.Sources[currentSourceId].QuitSignal <- true
						currentSourceId = i
					}
				}
			}
		case data := <- configuration.Sources[currentSourceId].StreamData:
		//write a chunk to stdout
			if _, err := os.Stdout.Write(data); err != nil {
				log.Fatal(err)

			}
		//data = data
		default:

		}
	}
}
