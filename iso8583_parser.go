package TxnLib

import (
	"encoding/xml"
	"os"
	"log"
	"encoding/hex"
	"strconv"
	"fmt"
	"errors"
	"encoding/json"
	"bytes"
	"encoding/gob"
	"time"
)

const hextable = "0123456789ABCDEF"

const (
	HeadMTI = iota
	HeadBitmap
)

var default_xml string = "{\"XMLName\":{\"Space\":\"\",\"Local\":\"ISO8583\"},\"Bits\":[{\"BitNo\":1,\"Type\":\"_ans\",\"DataContent\":\"16\",\"DataMaxSize\":\"\",\"BitName\":\"Bitmap\",\"Offset\":\"\"},{\"BitNo\":2,\"Type\":\"_ans\",\"DataContent\":\"VAR_LL\",\"DataMaxSize\":\"19\",\"BitName\":\"PrimaryAccNo\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":3,\"Type\":\"_ans\",\"DataContent\":\"6\",\"DataMaxSize\":\"\",\"BitName\":\"ProcessingCode\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":4,\"Type\":\"_ans\",\"DataContent\":\"12\",\"DataMaxSize\":\"\",\"BitName\":\"Amount\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":5,\"Type\":\"_ans\",\"DataContent\":\"12\",\"DataMaxSize\":\"\",\"BitName\":\"SettlAmount\",\"Offset\":\"\"},{\"BitNo\":6,\"Type\":\"_ans\",\"DataContent\":\"12\",\"DataMaxSize\":\"\",\"BitName\":\"IssuerAmount\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":7,\"Type\":\"_ans\",\"DataContent\":\"10\",\"DataMaxSize\":\"\",\"BitName\":\"DateTime\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":9,\"Type\":\"_ans\",\"DataContent\":\"8\",\"DataMaxSize\":\"\",\"BitName\":\"ExRate\",\"Offset\":\"\"},{\"BitNo\":10,\"Type\":\"_ans\",\"DataContent\":\"8\",\"DataMaxSize\":\"\",\"BitName\":\"ExchangeRate\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":11,\"Type\":\"_ans\",\"DataContent\":\"6\",\"DataMaxSize\":\"\",\"BitName\":\"SystemTraceNo\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":12,\"Type\":\"_ans\",\"DataContent\":\"6\",\"DataMaxSize\":\"\",\"BitName\":\"TimeLocal\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":13,\"Type\":\"_ans\",\"DataContent\":\"4\",\"DataMaxSize\":\"\",\"BitName\":\"DateLocal\",\"Offset\":\"4\",\"UseMac\":1},{\"BitNo\":14,\"Type\":\"_ans\",\"DataContent\":\"4\",\"DataMaxSize\":\"\",\"BitName\":\"ExpireDate\",\"Offset\":\"\"},{\"BitNo\":15,\"Type\":\"_ans\",\"DataContent\":\"4\",\"DataMaxSize\":\"\",\"BitName\":\"SettlmentDate\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":17,\"Type\":\"_ans\",\"DataContent\":\"4\",\"DataMaxSize\":\"\",\"BitName\":\"DataCaptured\",\"Offset\":\"4\",\"UseMac\":1},{\"BitNo\":18,\"Type\":\"_ans\",\"DataContent\":\"4\",\"DataMaxSize\":\"\",\"BitName\":\"MCCode\",\"Offset\":\"\"},{\"BitNo\":19,\"Type\":\"_an\",\"DataContent\":\"3\",\"DataMaxSize\":\"\",\"BitName\":\"CurrencyCode\",\"Offset\":\"\"},{\"BitNo\":22,\"Type\":\"_ans\",\"DataContent\":\"3\",\"DataMaxSize\":\"\",\"BitName\":\"PosEntryMode\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":23,\"Type\":\"_ans\",\"DataContent\":\"3\",\"DataMaxSize\":\"\",\"BitName\":\"CardSequence\",\"Offset\":\"\"},{\"BitNo\":24,\"Type\":\"_ans\",\"DataContent\":\"3\",\"DataMaxSize\":\"\",\"BitName\":\"NII\",\"Offset\":\"\"},{\"BitNo\":25,\"Type\":\"_ans\",\"DataContent\":\"2\",\"DataMaxSize\":\"\",\"BitName\":\"PosCondition\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":26,\"Type\":\"_ans\",\"DataContent\":\"2\",\"DataMaxSize\":\"\",\"BitName\":\"PinEntry\",\"Offset\":\"\"},{\"BitNo\":32,\"Type\":\"_ans\",\"DataContent\":\"VAR_LL\",\"DataMaxSize\":\"11\",\"BitName\":\"AcqCode\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":33,\"Type\":\"_ans\",\"DataContent\":\"VAR_LL\",\"DataMaxSize\":\"11\",\"BitName\":\"IssCode\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":34,\"Type\":\"_ans\",\"DataContent\":\"VAR_LL\",\"DataMaxSize\":\"19\",\"BitName\":\"ExtAccount\",\"Offset\":\"\"},{\"BitNo\":35,\"Type\":\"_ans\",\"DataContent\":\"VAR_LL\",\"DataMaxSize\":\"37\",\"BitName\":\"Track2\",\"Offset\":\"\"},{\"BitNo\":37,\"Type\":\"_an\",\"DataContent\":\"12\",\"DataMaxSize\":\"\",\"BitName\":\"RetrivalRefNo\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":38,\"Type\":\"_an\",\"DataContent\":\"6\",\"DataMaxSize\":\"\",\"BitName\":\"AuthIdResp\",\"Offset\":\"\"},{\"BitNo\":39,\"Type\":\"_an\",\"DataContent\":\"2\",\"DataMaxSize\":\"\",\"BitName\":\"ResponseCode\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":41,\"Type\":\"_ans\",\"DataContent\":\"8\",\"DataMaxSize\":\"\",\"BitName\":\"TerminalId\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":42,\"Type\":\"_ans\",\"DataContent\":\"15\",\"DataMaxSize\":\"\",\"BitName\":\"CardAcqId\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":43,\"Type\":\"_ans\",\"DataContent\":\"40\",\"DataMaxSize\":\"\",\"BitName\":\"CardAcqName\",\"Offset\":\"\"},{\"BitNo\":44,\"Type\":\"_ans\",\"DataContent\":\"VAR_LL\",\"DataMaxSize\":\"27\",\"BitName\":\"AuthInfo\",\"Offset\":\"\"},{\"BitNo\":45,\"Type\":\"_ans\",\"DataContent\":\"VAR_LL\",\"DataMaxSize\":\"76\",\"BitName\":\"Track1\",\"Offset\":\"\"},{\"BitNo\":48,\"Type\":\"_ans\",\"DataContent\":\"VAR_LLL\",\"DataMaxSize\":\"999\",\"BitName\":\"AddData\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":49,\"Type\":\"_an\",\"DataContent\":\"3\",\"DataMaxSize\":\"\",\"BitName\":\"CurrencyCode\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":50,\"Type\":\"_ans\",\"DataContent\":\"3\",\"DataMaxSize\":\"\",\"BitName\":\"SettCurCode\",\"Offset\":\"\"},{\"BitNo\":51,\"Type\":\"_an\",\"DataContent\":\"3\",\"DataMaxSize\":\"\",\"BitName\":\"IssuerCurrencyCode\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":52,\"Type\":\"_ans\",\"DataContent\":\"16\",\"DataMaxSize\":\"\",\"BitName\":\"PIN\",\"Offset\":\"\"},{\"BitNo\":53,\"Type\":\"_ans\",\"DataContent\":\"16\",\"DataMaxSize\":\"\",\"BitName\":\"SecurityControlInfo\",\"Offset\":\"\"},{\"BitNo\":54,\"Type\":\"_ans\",\"DataContent\":\"VAR_LLL\",\"DataMaxSize\":\"120\",\"BitName\":\"AditionalAmount\",\"Offset\":\"\"},{\"BitNo\":55,\"Type\":\"_ans\",\"DataContent\":\"VAR_LLL\",\"DataMaxSize\":\"255\",\"BitName\":\"ICCSysRelatedData\",\"Offset\":\"\"},{\"BitNo\":56,\"Type\":\"_ans\",\"DataContent\":\"VAR_LL\",\"DataMaxSize\":\"35\",\"BitName\":\"OrigDataElement\",\"Offset\":\"\"},{\"BitNo\":59,\"Type\":\"_ans\",\"DataContent\":\"VAR_LLL\",\"DataMaxSize\":\"999\",\"BitName\":\"TransInfo\",\"Offset\":\"\"},{\"BitNo\":60,\"Type\":\"_ans\",\"DataContent\":\"VAR_LLL\",\"DataMaxSize\":\"999\",\"BitName\":\"Private1\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":61,\"Type\":\"_ans\",\"DataContent\":\"VAR_LLL\",\"DataMaxSize\":\"999\",\"BitName\":\"Private2\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":62,\"Type\":\"_ans\",\"DataContent\":\"VAR_LLL\",\"DataMaxSize\":\"999\",\"BitName\":\"Private3\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":63,\"Type\":\"_ans\",\"DataContent\":\"VAR_LLL\",\"DataMaxSize\":\"999\",\"BitName\":\"Private4\",\"Offset\":\"\"},{\"BitNo\":64,\"Type\":\"_b\",\"DataContent\":\"16\",\"DataMaxSize\":\"\",\"BitName\":\"MAC\",\"Offset\":\"\"},{\"BitNo\":66,\"Type\":\"_ans\",\"DataContent\":\"1\",\"DataMaxSize\":\"\",\"BitName\":\"SettleCode\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":70,\"Type\":\"_ans\",\"DataContent\":\"3\",\"DataMaxSize\":\"\",\"BitName\":\"MNGCode\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":74,\"Type\":\"_ans\",\"DataContent\":\"10\",\"DataMaxSize\":\"\",\"BitName\":\"CreditCnt\",\"Offset\":\"\"},{\"BitNo\":75,\"Type\":\"_ans\",\"DataContent\":\"10\",\"DataMaxSize\":\"\",\"BitName\":\"CreditRevCnt\",\"Offset\":\"\"},{\"BitNo\":76,\"Type\":\"_ans\",\"DataContent\":\"10\",\"DataMaxSize\":\"\",\"BitName\":\"DebitCnt\",\"Offset\":\"\"},{\"BitNo\":77,\"Type\":\"_ans\",\"DataContent\":\"10\",\"DataMaxSize\":\"\",\"BitName\":\"DebitRevCnt\",\"Offset\":\"\"},{\"BitNo\":78,\"Type\":\"_ans\",\"DataContent\":\"10\",\"DataMaxSize\":\"\",\"BitName\":\"TransferCnt\",\"Offset\":\"\"},{\"BitNo\":79,\"Type\":\"_ans\",\"DataContent\":\"10\",\"DataMaxSize\":\"\",\"BitName\":\"TransferReverCnt\",\"Offset\":\"\"},{\"BitNo\":80,\"Type\":\"_ans\",\"DataContent\":\"10\",\"DataMaxSize\":\"\",\"BitName\":\"InqCnt\",\"Offset\":\"\"},{\"BitNo\":81,\"Type\":\"_ans\",\"DataContent\":\"10\",\"DataMaxSize\":\"\",\"BitName\":\"AuthCnt\",\"Offset\":\"\"},{\"BitNo\":82,\"Type\":\"_ans\",\"DataContent\":\"12\",\"DataMaxSize\":\"\",\"BitName\":\"CreditAmnt\",\"Offset\":\"\"},{\"BitNo\":83,\"Type\":\"_ans\",\"DataContent\":\"12\",\"DataMaxSize\":\"\",\"BitName\":\"CreditFeAmnt\",\"Offset\":\"\"},{\"BitNo\":84,\"Type\":\"_ans\",\"DataContent\":\"12\",\"DataMaxSize\":\"\",\"BitName\":\"DebitFeAmnt\",\"Offset\":\"\"},{\"BitNo\":85,\"Type\":\"_ans\",\"DataContent\":\"12\",\"DataMaxSize\":\"\",\"BitName\":\"DebitFeAmnt2\",\"Offset\":\"\"},{\"BitNo\":86,\"Type\":\"_ans\",\"DataContent\":\"16\",\"DataMaxSize\":\"\",\"BitName\":\"CreditAmnt\",\"Offset\":\"\"},{\"BitNo\":87,\"Type\":\"_ans\",\"DataContent\":\"16\",\"DataMaxSize\":\"\",\"BitName\":\"CreditRevAmnt\",\"Offset\":\"\"},{\"BitNo\":88,\"Type\":\"_ans\",\"DataContent\":\"16\",\"DataMaxSize\":\"\",\"BitName\":\"DebitAmnt\",\"Offset\":\"\"},{\"BitNo\":89,\"Type\":\"_ans\",\"DataContent\":\"16\",\"DataMaxSize\":\"\",\"BitName\":\"DebitRevAmnt\",\"Offset\":\"\"},{\"BitNo\":90,\"Type\":\"_ans\",\"DataContent\":\"42\",\"DataMaxSize\":\"\",\"BitName\":\"OrigData\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":95,\"Type\":\"_ans\",\"DataContent\":\"42\",\"DataMaxSize\":\"\",\"BitName\":\"ReplAmount\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":96,\"Type\":\"_ans\",\"DataContent\":\"16\",\"DataMaxSize\":\"\",\"BitName\":\"Security\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":97,\"Type\":\"_ans\",\"DataContent\":\"17\",\"DataMaxSize\":\"\",\"BitName\":\"AmountNet\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":99,\"Type\":\"_ans\",\"DataContent\":\"VAR_LL\",\"DataMaxSize\":\"11\",\"BitName\":\"SettlInst\",\"Offset\":\"\",\"UseMac\":1},{\"BitNo\":100,\"Type\":\"_ans\",\"DataContent\":\"VAR_LL\",\"DataMaxSize\":\"11\",\"BitName\":\"ReceivingInst\",\"Offset\":\"\"},{\"BitNo\":102,\"Type\":\"_ans\",\"DataContent\":\"VAR_LL\",\"DataMaxSize\":\"28\",\"BitName\":\"PriAccount\",\"Offset\":\"\"},{\"BitNo\":120,\"Type\":\"_ans\",\"DataContent\":\"VAR_LLL\",\"DataMaxSize\":\"999\",\"BitName\":\"ShetabAddData\",\"Offset\":\"\"},{\"BitNo\":121,\"Type\":\"_ans\",\"DataContent\":\"VAR_LLL\",\"DataMaxSize\":\"999\",\"BitName\":\"Private4\",\"Offset\":\"\"},{\"BitNo\":124,\"Type\":\"_ans\",\"DataContent\":\"VAR_LLL\",\"DataMaxSize\":\"999\",\"BitName\":\"StatementData\",\"Offset\":\"\"},{\"BitNo\":128,\"Type\":\"_b\",\"DataContent\":\"16\",\"DataMaxSize\":\"\",\"BitName\":\"MAC\",\"Offset\":\"\"}],\"Header\":[{\"FldNo\":0,\"BitName\":\"MsgType\",\"Type\":\"_ans\",\"Len\":4},{\"FldNo\":1,\"BitName\":\"Bitmap\",\"Type\":\"_ans\",\"Len\":16}]}"

type Iso8583Msg struct {
	Mti 		string
	Bitmap 		string
	MsgStruct	*Iso8583Struct
	Flds_value	map[int]*MsgValue
	Head_value	map[int]*MsgValue
}

type Iso8583Struct struct {
	Head_info	[]XMLHeader
	Flds_info	[]XMLBits
}

func (st *Iso8583Struct) GetFieldInfo( fldno int ) XMLBits  {
	return st.Flds_info[ fldno ]
}

func (st *Iso8583Struct) SetFieldInfo( fldno int , tmp XMLBits )   {
	st.Flds_info[fldno].copy(&tmp)
}

func (st *Iso8583Struct) GetHeadInfo( fldno int ) XMLHeader  {
	return st.Head_info[ fldno ]
}

func (st *Iso8583Struct) SetHeadInfo( fldno int , tmp XMLHeader )   {
	st.Head_info[fldno].copy(&tmp)
}

type MsgValue struct {
	Value string
}

func (m *MsgValue) Set( val string ){
	m.Value = val
}

type XMLBits struct {
	BitNo	int
	Type 	string
	DataContent 	string
	DataMaxSize 	string
	BitName		string
	Offset 		string
	UseMac		int
}

func (fld * XMLBits) copy( tmp * XMLBits ) {
	fld.BitNo = tmp.BitNo
	fld.Type = tmp.Type
	fld.DataContent = tmp.DataContent
	fld.DataMaxSize = tmp.DataMaxSize
	fld.BitName = tmp.BitName
	fld.Offset = tmp.Offset
	fld.UseMac = tmp.UseMac
}

type XMLHeader struct {
	FldNo	int
	BitName string
	Type 	string
	Len 	int
}

func (fld * XMLHeader) copy( tmp * XMLHeader ) {
	fld.Type = tmp.Type
	fld.BitName = tmp.BitName
	fld.Len = tmp.Len
	fld.FldNo = tmp.FldNo
}

type XMLStraps struct {
	XMLName  xml.Name    `xml:"ISO8583"`
	Bits   []XMLBits `xml:"tblBits"`
	Header 	[]XMLHeader `xml:"tblHeader"`
}

type errorString struct {
	s string
}

func (e *errorString) Error() string {
	return e.s
}

func CreateIso8583Struct(fname string) *Iso8583Struct {
	st := Iso8583Struct{}
	st.Head_info = make([]XMLHeader,10)
	st.Flds_info = make([]XMLBits,130)
	err := st.GetConfig(fname)
	if err != nil {
		return nil
	}
	return &st
}

func CreateIso8583Msg( mti string , st * Iso8583Struct ) *Iso8583Msg {
	msg := Iso8583Msg{Mti:mti,MsgStruct:st}
	msg.Head_value = make(map[int]*MsgValue)
	msg.Flds_value = make(map[int]*MsgValue)
	msg.SetHeadValue(HeadMTI,mti)
	return &msg
}

func CreateIso8583MsgFromBytes( data []byte , st * Iso8583Struct ) *Iso8583Msg {
	msg := Iso8583Msg{MsgStruct:st}
	msg.Head_value = make(map[int]*MsgValue)
	msg.Flds_value = make(map[int]*MsgValue)
	msg.Parse(data)
	return &msg
}

func CheckBitmap( bmp string, sbmp string, fldno int ) bool {
	var fld int
	fld = fldno
	if fldno > 64  {
		if( len(sbmp) ) > 0 {
			byte_bmp, err := hex.DecodeString(sbmp)
			if err == nil {
				return getBit(byte_bmp, fld-64)
			}
		}else{
			return false
		}
	}else{
		byte_bmp, err := hex.DecodeString(bmp)
		if err == nil {
			return getBit(byte_bmp, fld)
		}

	}
	return false
}

func SetBitmap( bmp *string , fldno int ) {
	bt, _ := hex.DecodeString(*bmp)
	setBit(&bt,fldno)
	*bmp = hex.EncodeToString(bt)
}

func getBit( bitmap []byte , fldno int ) bool {
	var fldByte int
	fldByte = (fldno - 1) / 8
	step := uint(8 - (fldno-(fldByte*8)))
	if bitmap[fldByte] & (0x01 << step) == 0 {
		return false
	}else{
		return true
	}
}

func setBit( bitmap *[]byte , fldno int ) {
	var fldByte int
	fldByte = (fldno - 1) / 8
	step := uint(8 - (fldno-(fldByte*8)))
	(*bitmap)[fldByte] |= (0x01 << step)
}

func EncodedLen(n int) int { return n * 2 }

func Encode(dst, src []byte) int {
	for i, v := range src {
		dst[i*2] = hextable[v>>4]
		dst[i*2+1] = hextable[v&0x0f]
	}


	return len(src) * 2
}

func EncodeToString(src []byte) string {
	dst := make([]byte, EncodedLen(len(src)))
	Encode(dst, src)
	return string(dst)
}

func (iso *Iso8583Msg) IsRequest() bool {
	var mti uint16
	fmt.Sscanf(iso.Mti,"%04x",&mti)
	if mti&0x0010 == 0 {
		return true
	}
	return false
}

func (iso *Iso8583Msg) setField( i int, data *[]byte, offset *int, real_bitmap *[]byte ) {
	var m_len int
	var err error
	switch iso.MsgStruct.GetFieldInfo( i ).DataContent {
	case "VAR_LL":
		m_len = iso.GetFieldLen(i)
		copy((*data)[(*offset):(*offset)+2],fmt.Sprintf("%02d",m_len))
		(*offset) += 2
	case "VAR_LLL":
		m_len = iso.GetFieldLen(i)
		copy((*data)[(*offset):(*offset)+3],fmt.Sprintf("%03d",m_len))
		(*offset) += 3
	default:
		m_len, err = strconv.Atoi(iso.MsgStruct.GetFieldInfo( i ).DataContent)
	}
	if err != nil {
		log.Println("len set error", i,err,iso.MsgStruct.GetFieldInfo( i ))
	}
	str := iso.GetFieldValue(i)
	switch iso.MsgStruct.GetFieldInfo( i ).Type {
	case "_ans":
		copy((*data)[(*offset):(*offset)+m_len] , []byte(str))
	case "_an":
		copy((*data)[(*offset):(*offset)+m_len] , []byte(str))
	case "_b":
		copy((*data)[(*offset):(*offset)+m_len] , []byte(str))
	}

	(*offset) += m_len
	//log.Printf("fld[%d]-->[%s]\n", i, str)
	if i>64 {
		setBit(real_bitmap, i-64)
	}else {
		setBit(real_bitmap, i)
	}

}

func (iso *Iso8583Msg) getField( i int , data []byte , offset * int ) {
	var len int
	var err error
	switch iso.MsgStruct.GetFieldInfo( i ).DataContent {
	case "VAR_LL":
		len, err = strconv.Atoi(string(data[(*offset):(*offset)+2]))
		(*offset) += 2
	case "VAR_LLL":
		len, err = strconv.Atoi(string(data[(*offset):(*offset)+3]))
		(*offset) += 3
	default:
		len, err = strconv.Atoi(iso.MsgStruct.GetFieldInfo( i ).DataContent)
	}
	if err != nil {
		log.Println("len parse error", i,err,iso.MsgStruct.GetFieldInfo(i))
	}
	switch iso.MsgStruct.GetFieldInfo( i ).Type {
	case "_ans":
		iso.SetFieldValue(i,string(data[(*offset):(*offset)+len]))
	case "_an":
		iso.SetFieldValue(i,string(data[(*offset):(*offset)+len]))
	case "_b":
		iso.SetFieldValue(i,string(data[(*offset):(*offset)+len]))
	}
	(*offset) += len
}

func (iso *Iso8583Msg) getHead( i int , data []byte, offset * int ) {
	mlen := iso.MsgStruct.GetHeadInfo(i).Len
	switch iso.MsgStruct.GetHeadInfo(i).Type {
	case "_ans":
		iso.SetHeadValue(i,string(data[(*offset):(*offset)+mlen]))
	}
	(*offset) += mlen
}

func (iso *Iso8583Struct) GetConfig(fname string) error {
	file, err := os.Open(fname)
	var xmlStrap XMLStraps
	if err != nil {
		json.Unmarshal([]byte(default_xml),&xmlStrap)
		for _, head := range xmlStrap.Header {
			log.Println(head)
			iso.SetHeadInfo(head.FldNo,head)
		}
		for _, fld:= range xmlStrap.Bits {
			// sort by BitNo to array
			iso.SetFieldInfo(fld.BitNo,fld)
		}
		log.Println("loaded iso from cache")
		return nil
	}else {
		err2 := xml.NewDecoder(file).Decode(&xmlStrap)
		if err2 != nil {
			log.Println(fname,err2)
			return err2
		}else{
			bt,_ := json.Marshal(xmlStrap)
			log.Println(string(bt))
			for _, head := range xmlStrap.Header {
				log.Println(head)
				iso.SetHeadInfo(head.FldNo,head)
			}
			for _, fld:= range xmlStrap.Bits {
				// sort by BitNo to array
				iso.SetFieldInfo(fld.BitNo,fld)
			}

			log.Println(fname,"loaded")
			return nil
		}


	}

}

func (iso *Iso8583Msg) Parse(data []byte) {
	var i int = 0
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in Parse", i,r,string(data))
		}
	}()
	offset := 0

	head_cnt := len(iso.MsgStruct.Head_info)

	for i=0;i<head_cnt;i++ {
		if iso.MsgStruct.Head_info[i].Len > 0 {
			iso.getHead(i,data,&offset)
			switch iso.MsgStruct.Head_info[i].BitName {
			case "MsgType":
				iso.Mti, _ = iso.GetHeadValue(i)
			case "Bitmap":
				iso.Bitmap, _ = iso.GetHeadValue(i)
			}
		}
	}

	bitmap,_ := hex.DecodeString(iso.Bitmap)

	for i=1;i<=64&&offset<len(data);i++ {
		if getBit(bitmap,i) {
			iso.getField(i, data, &offset)
		}
	}
	if iso.GetFieldLen(1) > 0 {
		str := iso.GetFieldValue(1)
		second_bitmap,_ := hex.DecodeString(str)
		for i=65;i<=128&&offset<len(data);i++ {
			if getBit(second_bitmap,i-64) {
				iso.getField(i, data, &offset)
			}
		}
	}
}

func (iso *Iso8583Msg) MakeFixMsg(bmp string) []byte {
	real_bitmap := make([]byte,8)
	data := make([]byte,1500)
	offset := 0

	head_cnt := len(iso.MsgStruct.Head_info)

	var bitmap_offset int

	for i:=0;i<head_cnt;i++ {
		mlen := iso.MsgStruct.Head_info[i].Len
		if mlen > 0 {
			switch iso.MsgStruct.Head_info[i].Type {
			case "_ans":
				switch iso.MsgStruct.Head_info[i].BitName {
				case "MsgType":
					copy(data[offset:offset+mlen],iso.Mti)
				case "Bitmap":
					copy(data[offset:offset+mlen],iso.Bitmap)
					bitmap_offset = offset
				default:
					str,_ := iso.GetHeadValue(i)
					copy(data[offset:offset+mlen],[]byte(str))
				}
				offset += mlen
			}
		}
	}


	bitmap,_ := hex.DecodeString(string(bmp))
	second_bitmap_offset := offset
	for i:=1;i<=64;i++ {
		if( getBit(bitmap,i) ){
			iso.setField( i, &data , &offset , &real_bitmap )
		}
	}
	iso.SetHeadValue(HeadBitmap,EncodeToString(real_bitmap))  // also set iso.Bitmap inside this function
	copy( data[bitmap_offset:bitmap_offset+16] , iso.Bitmap )
	if getBit(bitmap,1) && iso.GetFieldLen(1) > 0 {
		real_second_bitmap := make([]byte,8)
		str:= iso.GetFieldValue(1)
		second_bitmap,_ := hex.DecodeString(str)
		for i:=65;i<=128;i++ {
			if getBit(second_bitmap,i-64) {
				iso.setField( i, &data , &offset , &real_second_bitmap )
			}
		}
		iso.SetFieldValue( 1, EncodeToString(real_second_bitmap) )
		copy( data[second_bitmap_offset:second_bitmap_offset+16] , EncodeToString(real_second_bitmap) )
	}
	data2 := make([]byte,offset)
	copy(data2,data)
	return data2
}

func (iso *Iso8583Msg) MakeFixMsgRemoveEmpty(bmp string) []byte {
	real_bitmap := make([]byte,8)
	data := make([]byte,1500)
	offset := 0

	head_cnt := len(iso.MsgStruct.Head_info)

	var bitmap_offset int

	for i:=0;i<head_cnt;i++ {
		mlen := iso.MsgStruct.Head_info[i].Len
		if mlen > 0 {
			switch iso.MsgStruct.Head_info[i].Type {
			case "_ans":
				switch iso.MsgStruct.Head_info[i].BitName {
				case "MsgType":
					copy(data[offset:offset+mlen],iso.Mti)
				case "Bitmap":
					copy(data[offset:offset+mlen],iso.Bitmap)
					bitmap_offset = offset
				default:
					str,_ := iso.GetHeadValue(i)
					copy(data[offset:offset+mlen],[]byte(str))
				}
				offset += mlen
			}
		}
	}


	bitmap,_ := hex.DecodeString(string(bmp))
	second_bitmap_offset := offset
	for i:=1;i<=64;i++ {
		if getBit(bitmap,i) && iso.GetFieldLen(i)>0 {
			iso.setField( i, &data , &offset , &real_bitmap )
		}
	}
	iso.SetHeadValue(HeadBitmap,EncodeToString(real_bitmap))  // also set iso.Bitmap inside this function
	copy( data[bitmap_offset:bitmap_offset+16] , iso.Bitmap )
	if getBit(bitmap,1) && iso.GetFieldLen(1) > 0 {
		real_second_bitmap := make([]byte,8)
		str:= iso.GetFieldValue(1)
		second_bitmap,_ := hex.DecodeString(str)
		for i:=65;i<=128;i++ {
			if getBit(second_bitmap,i-64) && iso.GetFieldLen(i)>0 {
				iso.setField( i, &data , &offset , &real_second_bitmap )
			}
		}
		iso.SetFieldValue( 1, EncodeToString(real_second_bitmap) )
		copy( data[second_bitmap_offset:second_bitmap_offset+16] , EncodeToString(real_second_bitmap) )
	}
	data2 := make([]byte,offset)
	copy(data2,data)
	return data2
}


func (iso *Iso8583Msg) Make() []byte {
	real_bitmap := make([]byte,8)
	data := make([]byte,1500)
	offset := 0

	head_cnt := len(iso.MsgStruct.Head_info)

	var bitmap_offset int

	for i:=0;i<head_cnt;i++ {
		mlen := iso.MsgStruct.Head_info[i].Len
		if mlen > 0 {
			switch iso.MsgStruct.Head_info[i].Type {
			case "_ans":
				switch iso.MsgStruct.Head_info[i].BitName {
				case "MsgType":
					copy(data[offset:offset+mlen],iso.Mti)
				case "Bitmap":
					copy(data[offset:offset+mlen],iso.Bitmap)
					bitmap_offset = offset
				default:
					str,_ := iso.GetHeadValue(i)
					copy(data[offset:offset+mlen],[]byte(str))
				}
				offset += mlen
			}
		}
	}

	//copy(data[offset:],iso.Mti)
	//offset += 4
	//iso.loc_logger.Printf("MTI   --->[%s]",iso.Mti)
	//bitmap_offset := offset
	//offset += 16

	second_bitmap_offset := offset
	for i:=1;i<=64;i++ {
		if iso.GetFieldLen(i) > 0 {
			iso.setField( i, &data , &offset , &real_bitmap )
		}
	}
	iso.SetHeadValue(HeadBitmap,EncodeToString(real_bitmap))
	copy( data[bitmap_offset:bitmap_offset+16] , iso.Bitmap )
	if iso.GetFieldLen(1) > 0 {
		real_second_bitmap := make([]byte,8)
		for i:=65;i<=128;i++ {
			if iso.GetFieldLen(i) > 0 {
				iso.setField( i, &data , &offset , &real_second_bitmap )
			}
		}
		iso.SetFieldValue( 1, EncodeToString(real_second_bitmap) )
		copy( data[second_bitmap_offset:second_bitmap_offset+16] , EncodeToString(real_second_bitmap) )
	}
	data2 := make([]byte,offset)
	copy(data2,data)
	return data2
}


func (iso *Iso8583Msg) SetFieldValue( fldno int , value string ) {
	iso.Flds_value[ fldno ] = &MsgValue{Value:value}
}

func (iso *Iso8583Msg) GetFieldValue( fldno int ) string {
	tmp := iso.Flds_value[ fldno ]
	if tmp == nil {
		return ""
	}
	return tmp.Value
}

func (iso *Iso8583Msg) GetFieldLen( fldno int ) int {
	if iso.Flds_value[ fldno ] == nil {
		return 0
	}else{
		return len(iso.Flds_value[ fldno ].Value)
	}

}

func (iso *Iso8583Msg) SetHeadValue( fldno int , value string ) {
	iso.Head_value[ fldno ] = &MsgValue{Value:value}
	switch fldno {
	case HeadMTI:
		iso.Mti = value
	case HeadBitmap:
		iso.Bitmap = value

	}
}

func (iso *Iso8583Msg) GetHeadValue( fldno int ) (string, error) {
	tmp := iso.Head_value[ fldno ]
	if tmp == nil {
		return "", errors.New(fmt.Sprintf("Fld %s is empty",fldno))

	}else{
		return tmp.Value, nil
	}
}


func (iso *Iso8583Msg) GetHeadLen( fldno int ) int {
	if iso.Head_value[ fldno ] == nil {
		return 0
	}else{
		return len(iso.Head_value[ fldno ].Value)
	}

}


func (iso *Iso8583Msg) Dump() {
	//tmp.Printf("MTI  -->[%s]\n",iso.Mti)
	//tmp.Printf("Bitmap->[%s]\n",iso.Bitmap)
	var i int
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in Dump", i,r)
		}
	}()
	var out_str string
	now := time.Now()
	out_str = fmt.Sprintf("%04d%02d%02d %02d:%02d:%02d.%d\n",now.Year(),now.Month(),now.Day(),now.Hour(),now.Minute(),now.Second(),now.Nanosecond())
	for i=0;i<len(iso.MsgStruct.Head_info);i++ {
		if iso.GetHeadLen(i) > 0 {
			str, _ := iso.GetHeadValue(i)
			out_str = fmt.Sprintf("%sHEAD[%d]-->[%s]\n",out_str,i,str)
			//log.Printf("HEAD[%d]-->[%s]\n", i, str)
		}
	}
	for i=1;i<=128;i++ {
		if CheckBitmap(iso.Bitmap,iso.GetFieldValue(1),i) && i!=52 {
			str := iso.GetFieldValue(i)
			if i == 2 && len(str)>=16 {
				str = fmt.Sprintf("%6s****%0s",string(str[0:6]),string(str[12:]))
			}
			out_str = fmt.Sprintf("%sFLD[%d]-->[%s]\n",out_str,i,str)
			//log.Printf("FLD[%d]-->[%s]\n", i, str)
		}
	}
	log.Println(out_str)
}

func (iso *Iso8583Msg) GetKey() string {
	var key string
	var mti uint16
	fmt.Sscanf(iso.Mti,"%04x",&mti)
	mti = mti&0xFFEF
	switch mti {
	case 0x200:
		key = fmt.Sprintf("%04x:%s:%s",mti,iso.GetFieldValue(41),iso.GetFieldValue(11))
		log.Printf("transaction key [%s]\n",key)
	case 0x220:
		key = fmt.Sprintf("%04x:%s:%s",mti,iso.GetFieldValue(41),iso.GetFieldValue(11))
		log.Printf("transaction key [%s]\n",key)
	case 0x400:
		key = fmt.Sprintf("%04x:%s:%s",mti,iso.GetFieldValue(2),iso.GetFieldValue(11))
	case 0x420:
		key = fmt.Sprintf("%04x:%s:%s",mti,iso.GetFieldValue(2),iso.GetFieldValue(11))
	default:
		key = fmt.Sprintf("%04x:%s",mti,iso.GetFieldValue(11))
		log.Printf("transaction key [%s]\n",key)
	}
	return key
}

func (iso *Iso8583Msg) ToJson() []byte {
	b,err := json.Marshal(iso)
	if err!= nil {
		log.Println(err)
		return  nil
	}else{
		return b
	}
}

func (iso *Iso8583Msg) ToGOB() []byte {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	err := e.Encode(iso)
	if err != nil { fmt.Println(`failed gob Encode`, err) }
	return b.Bytes()
}

func (iso *Iso8583Msg) GetMacString7() string {
	var mac_string string
	for i:=2;i<=128;i++ {
		if CheckBitmap(iso.Bitmap,iso.GetFieldValue(1),i) && iso.GetFieldLen(i)>0 && iso.MsgStruct.GetFieldInfo(i).UseMac == 1 {
			mac_string = fmt.Sprintf("%s%s",mac_string,iso.GetFieldValue(i))
		}
	}
	return mac_string
}

func CreateIso8583MsgFromGOB(tmp []byte) *Iso8583Msg {
	m := Iso8583Msg{}

	b := bytes.Buffer{}
	b.Write(tmp)
	d := gob.NewDecoder(&b)
	err := d.Decode(&m)
	if err != nil { fmt.Println(`failed gob Decode`, err); }
	return &m
}