package main


import (
    "fmt"
    "net/http"
    "encoding/json"
    "io/ioutil"
    "hash/crc32"
    "sort"
)

type KeyValue struct{
    Key int `json:"key,omitempty"`
    Value string `json:"value,omitempty"`
}
type HashCircle []uint32

func (ch HashCircle) Len() int {
    return len(ch)
}

func (ch HashCircle) Less(i, j int) bool {
    return ch[i] < ch[j]
}

func (ch HashCircle) Swap(i, j int) {
    ch[i], ch[j] = ch[j], ch[i]
}

type Node struct {
    Id  int
    IP  string
}

func NewNode(id int, ip string) *Node {
    return &Node{
        Id: id,
        IP: ip,
    }
}

type ConsistentHash struct {
    Nodes       map[uint32]Node
    IsPresent   map[int]bool
    Circle      HashCircle
}

func NewConsistentHash() *ConsistentHash {
    return &ConsistentHash{
        Nodes:      make(map[uint32]Node),
        IsPresent:  make(map[int]bool),
        Circle:     HashCircle{},
    }
}

func (ch *ConsistentHash) AddNode(node *Node) bool {
    if _, ok := ch.IsPresent[node.Id]; ok {
        return false
    }
    str := ch.ReturnNodeIP(node)
    ch.Nodes[ch.GetHashValue(str)] = *(node)
    ch.IsPresent[node.Id] = true
    ch.SortHashCircle()
    return true
}

func (ch *ConsistentHash) SortHashCircle() {
    ch.Circle = HashCircle{}
    for k := range ch.Nodes {
        ch.Circle = append(ch.Circle, k)
    }
    sort.Sort(ch.Circle)
}

func (ch *ConsistentHash) ReturnNodeIP(node *Node) string {
    return node.IP
}

func (ch *ConsistentHash) GetHashValue(key string) uint32 {
    return crc32.ChecksumIEEE([]byte(key))
}

func (ch *ConsistentHash) Get(key string) Node {
    hash := ch.GetHashValue(key)
    i := ch.SearchForNode(hash)
    return ch.Nodes[ch.Circle[i]]
}

func (ch *ConsistentHash) SearchForNode(hash uint32) int {
    i := sort.Search(len(ch.Circle), func(i int) bool {return ch.Circle[i] >= hash })
    if i < len(ch.Circle) {
        if i == len(ch.Circle)-1 {
            return 0
        } else {
            return i
        }
    } else {
        return len(ch.Circle) - 1
    }
}

func PutKey(circle *ConsistentHash, str string, input string){
        fmt.Println("\nPUT: " +str+"==>"+input)
        ipAddress := circle.Get(str)
        address := "http://"+ipAddress.IP+"/keys/"+str+"/"+input
		fmt.Println(address)
        req,err := http.NewRequest("PUT",address,nil)
        client := &http.Client{}
        resp, err := client.Do(req)
        if err!=nil{
            fmt.Println("Error:",err)
        }else{
            defer resp.Body.Close()
            fmt.Println("Response : 200 OK")
        }
}

func GetKey(key string,circle *ConsistentHash){
    var out KeyValue
    ipAddress:= circle.Get(key)
	address := "http://"+ipAddress.IP+"/keys/"+key
	fmt.Println("\n"+address)
    response,err:= http.Get(address)
    if err!=nil{
        fmt.Println("Error:",err)
    }else{
        defer response.Body.Close()
        contents,err:= ioutil.ReadAll(response.Body)
        if(err!=nil){
            fmt.Println(err)
        }
        json.Unmarshal(contents,&out)
        result,_:= json.Marshal(out)
        fmt.Println(string(result))
    }
}

func GetAll(address string){
    var out []KeyValue
    response,err:= http.Get(address)
    if err!=nil{
        fmt.Println("Error:",err)
    }else{
        defer response.Body.Close()
        contents,err:= ioutil.ReadAll(response.Body)
        if(err!=nil){
            fmt.Println(err)
        }
        json.Unmarshal(contents,&out)
        result,_:= json.Marshal(out)
        fmt.Println(string(result))
    }
}
func main() {
    circle := NewConsistentHash()
    circle.AddNode(NewNode(0, "127.0.0.1:3000"))
	circle.AddNode(NewNode(1, "127.0.0.1:3001"))
	circle.AddNode(NewNode(2, "127.0.0.1:3002"))

    fmt.Println("\n\n****PUT Keys****")
    PutKey(circle,"1","a")
    PutKey(circle,"2","b")
    PutKey(circle,"3","c")
    PutKey(circle,"4","d")
    PutKey(circle,"5","e")
    PutKey(circle,"6","f")
    PutKey(circle,"7","g")
    PutKey(circle,"8","h")
    PutKey(circle,"9","i")
    PutKey(circle,"10","j")

    fmt.Println("\n\n****GET Keys****")
    GetKey("1",circle)
    GetKey("2",circle)
    GetKey("3",circle)
    GetKey("4",circle)
    GetKey("5",circle)
    GetKey("6",circle)
    GetKey("7",circle)
    GetKey("8",circle)
    GetKey("9",circle)
    GetKey("10",circle)

    fmt.Println("\n\n****GET Keys from 127.0.0.1:3000****\n")
    GetAll("http://127.0.0.1:3000/keys")
    fmt.Println("\n\n****GET Keys from 127.0.0.1:3001****\n")
    GetAll("http://127.0.0.1:3001/keys")
    fmt.Println("\n\n****GET Keys from 127.0.0.1:3002****\n")
    GetAll("http://127.0.0.1:3002/keys")
}
