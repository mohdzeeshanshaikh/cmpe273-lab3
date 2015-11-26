package main
import  (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sort"
	"encoding/json"
	"github.com/julienschmidt/httprouter"
)

type KeyValue struct{
	Key int	`json:"key,omitempty"`
	Value string	`json:"value,omitempty"`
}

var z1,z2,z3 [] KeyValue
var idx1,idx2,idx3 int
type byKey []KeyValue
func (a byKey) Len() int           { return len(a) }
func (a byKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


func GetAllKeys(rw http.ResponseWriter, request *http.Request,p httprouter.Params){
	port := strings.Split(request.Host,":")
	if(port[1]=="3000"){
		sort.Sort(byKey(z1))
		result,_:= json.Marshal(z1)
		fmt.Fprintln(rw,string(result))
	}else if(port[1]=="3001"){
		sort.Sort(byKey(z2))
		result,_:= json.Marshal(z2)
		fmt.Fprintln(rw,string(result))
	}else{
		sort.Sort(byKey(z3))
		result,_:= json.Marshal(z3)
		fmt.Fprintln(rw,string(result))
	}
}

func PutKeys(rw http.ResponseWriter, request *http.Request,p httprouter.Params){
	port := strings.Split(request.Host,":")
	key,_ := strconv.Atoi(p.ByName("key_id"))
	if(port[1]=="3000"){
		z1 = append(z1,KeyValue{key,p.ByName("value")})
		idx1++
	}else if(port[1]=="3001"){
		z2 = append(z2,KeyValue{key,p.ByName("value")})
		idx2++
	}else{
		z3 = append(z3,KeyValue{key,p.ByName("value")})
		idx3++
	}
}

func GetKey(rw http.ResponseWriter, request *http.Request,p httprouter.Params){
	out := z1
	ind := idx1
	port := strings.Split(request.Host,":")
	if(port[1]=="3001"){
		out = z2
		ind = idx2
	}else if(port[1]=="3002"){
		out = z3
		ind = idx3
	}
	key,_ := strconv.Atoi(p.ByName("key_id"))
	for i:=0 ; i< ind ;i++{
		if(out[i].Key==key){
			result,_:= json.Marshal(out[i])
			fmt.Fprintln(rw,string(result))
		}
	}
}



func main(){
	idx1 = 0
	idx2 = 0
	idx3 = 0
	router := httprouter.New()
    router.GET("/keys",GetAllKeys)
    router.GET("/keys/:key_id",GetKey)
    router.PUT("/keys/:key_id/:value",PutKeys)
    go http.ListenAndServe(":3000",router)
    go http.ListenAndServe(":3001",router)
    go http.ListenAndServe(":3002",router)
    select {}
}
