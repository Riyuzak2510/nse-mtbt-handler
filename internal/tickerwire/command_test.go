package tickerwire
import("bytes";"testing";"time";"nse-mtbt-handler/internal/feed";"nse-mtbt-handler/internal/runconfig")
func wireManifest()runconfig.Manifest{return runconfig.Manifest{Version:2,Profile:feed.Profile,StreamID:83,Seed:42,Baseline:"empty",CreatedAt:time.Now().UTC(),Instruments:[]runconfig.Instrument{{Token:100,Symbol:"A",TickSize:5,BasePrice:1000},{Token:101,Symbol:"B",TickSize:5,BasePrice:1000}}}}
func wireID()feed.OrderID{id,_:=feed.IDFromFloat64(1);return id}
func wireRaw(t *testing.T,m runconfig.Manifest)[]byte{t.Helper();raw,e:=feed.Encode(feed.Packet{Stream:m.StreamID,Type:feed.New,Sequence:1,Timestamp:1,OrderID:wireID(),Token:100,Side:feed.Buy,Price:1000,Quantity:10});if e!=nil{t.Fatal(e)};return raw}
func TestCommandRoundTripAndChain(t *testing.T){m:=wireManifest();raw:=wireRaw(t,m);c,e:=New(m,100,1,1,time.Now().UTC(),raw,"");if e!=nil{t.Fatal(e)};b,e:=Encode(c);if e!=nil{t.Fatal(e)};d,e:=Decode(b);if e!=nil||Validate(m,d)!=nil{t.Fatal(e)};if d.Digest!=c.Digest||ID(c)==""||Subject(83,100)!="mtbtmatch.83.100"||StreamName(83)!="MATCH_83"{t.Fatal(d)};c2,e:=New(m,100,2,2,c.Received,raw,c.Digest);if e!=nil||c2.Digest==c.Digest{t.Fatal(e)};raw[0]^=1;if bytes.Equal(raw,c.Raw){t.Fatal("borrowed source buffer")}}
func TestCommandStrictValidation(t *testing.T){m:=wireManifest();raw:=wireRaw(t,m);c,e:=New(m,100,1,1,time.Now().UTC(),raw,"");if e!=nil{t.Fatal(e)}
 for _,mutate:=range []func(*Command){func(c *Command){c.Token=101},func(c *Command){c.Sequence=2},func(c *Command){c.SourceBroker=0},func(c *Command){c.Digest="bad"},func(c *Command){c.StreamID=1},func(c *Command){c.Raw[0]^=1},func(c *Command){c.Version=2},func(c *Command){c.PreviousDigest=c.Digest}}{d:=c;d.Raw=append([]byte(nil),c.Raw...);mutate(&d);if Validate(m,d)==nil{t.Fatal("bad command accepted")}}
 b,_:=Encode(c);for _,bad:=range [][]byte{append(append([]byte(nil),b...),[]byte(" {}")...),[]byte(`{"unknown":1}`),bytes.Repeat([]byte("x"),MaxCommandBytes+1)}{if _,e:=Decode(bad);e==nil{t.Fatal("bad JSON accepted")}}
 if _,e:=New(m,0,0,1,time.Now(),nil,"");e==nil{t.Fatal("fake heartbeat accepted")}
 hb,_:=feed.Encode(feed.Packet{Stream:m.StreamID,Type:feed.Heartbeat,LastSequence:1});if _,e:=New(m,101,2,2,c.Received,hb,c.Digest);e!=nil{t.Fatal("per ticker control rejected",e)}
}
