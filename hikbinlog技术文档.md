# Hikbinlog（二进制日志）技术文档

## 一、概述

本技术文档描述了的hikbinlog（二进制日志）的设计目的、实现流程、迭代过程、测试结果。

## 二、设计目的

### **2.1 高效存储**：

二进制日志以紧凑的格式存储数据，相比于文本格式，能够节省存储空间，提高数据的写入和读取效率。

### **2.2 易于压缩**：

二进制数据往往比文本数据更容易进行压缩，从而进一步节省存储空间和提高传输效率。

### **2.3 便于后续处理**：

二进制日志可以方便地与其他系统或工具集成，例如用于数据分析、监控，提升数据的可用性。

### **2.4 可扩展性**：

二进制日志的设计允许未来添加新的数据字段或结构，而不会影响现有的日志解析，增强系统的可扩展性。

## 三、实现流程

### 3.1 逻辑流程

![](.\hikbinlog实现流程.png)

### 3.2 数据流程

![](.\hikbinlog实现流程2.png)

## 四、迭代过程

### 4.1 日志事件格式文件迭代

#### 4.1.1 Android Eventlog日志事件格式介绍

安卓的EventLog中的日志存储格式（EventTag）：

```txt
EventTag_Id  EventTag_Name  EventTag_Ele1(Name|type|unit),EventTag_Ele2(Name|type|unit),...
事件ID  事件名  事件元素1(元素名|元素数据类型|元素单位)，事件元素2，...
```

type取值的含义：

```txt
1: int
2: long
3: string
4: list
5: float
```

unit取值的含义：

```txt
1: Number of objects(对象个数)
2: Number of bytes(字节数)
3: Number of milliseconds(毫秒)
4: Number of allocations(分配个数)
5: Id(Id号)
6: Percent(百分比)
s: Number of seconds (秒)
```

实例：

```txt
2724 power_sleep_requested (wakeLocksCleared|1|1)
2725 power_screen_broadcast_send (wakelockCount|1|1)
2726 power_screen_broadcast_done (on|1|5),(broadcastDuration|2|3),(wakelockCount|1|1)
2727 power_screen_broadcast_stop (which|1|5),(wakelockCount|1|1)
2728 power_screen_state (offOrOn|1|5),(becauseOfUser|1|5),(totalTouchDownTime|2|3),(touchCycles|1|1)
2729 power_partial_wake_state (releasedorAcquired|1|5),(tag|3)
```

#### 4.1.2 Hikbinlog日志事件格式设计

参考安卓的Eventlog格式，为实现日志存储空间的节省，对其进行了一定的修改：

```txt
EventTag_Id  EventTag_Name  EventTag_Ele1(Name|type|bits),EventTag_Ele2(Name|type|bits),...
事件ID  事件名  事件元素1(元素名|元素数据类型|元素长度)，事件元素2，...
```

type取值的含义：

```txt
L: long
D: double
S: string
```

bits取值的含义：

```txt
n: 该元素占n个bit
```

实例：

```txt
2724 power_sleep_requested (wakeLocksCleared|L|16)
2725 power_screen_broadcast_send (wakelockCount|L|8)
2726 power_screen_broadcast_done (on|L|16),(broadcastDuration|D|64),(wakelockCount|L|8)
2727 power_screen_broadcast_stop (which|L|16),(wakelockCount|L|8)
2728 power_screen_state (offOrOn|L|8),(becauseOfUser|L|16),(totalTouchDownTime|D|64),(touchCycles|L|24)
2729 power_partial_wake_state (releasedorAcquired|L|16),(tag|S|48)
```

#### 4.1.3 Hikbinlog日志事件格式文件设计

**（1）**将所有日志事件存储在同一个文件eventlogst.txt中，并存放到指定位置；事件ID需连续。

```
|--eventlogst.txt
```

**（2）**支持将日志事件存储在不同文件中（eventlogst_n.txt),但存放到同一目录eventlogst下，并存放到指定位置；事件ID无需连续。

```
|--eventlogst
       |--eventlogst_1.txt
       |--eventlogst_2.txt
       |--eventlogst_3.txt
```

### 4.2 日志事件存储数据结构迭代

#### 4.2.1 为每一个日志事件生成对应的结构体

读取 **“2728 power_screen_state (offOrOn|L|8),(becauseOfUser|L|16),(totalTouchDownTime|D|64),(touchCycles|L|24)”** 并生成：

```c
struct POWER_SCREEN_STATE {
	long offOrOn : 8;
	long becauseOfUser : 16;
	double totalTouchDownTime;
	long touchCycles : 24;
}
```

读取 **“2729 power_partial_wake_state (releasedorAcquired|L|16),(tag|S|48)”** 并生成：

```c
struct POWER_PARTIAL_WAKE_STATE {
	long releasedorAcquired : 16;
	char tag[6];
}
```

#### 4.2.2 为所有日志事件设计通用结构体（EventTag）

结构体EventTag具体内容：

```c
typedef struct {
    unsigned int          tag_index;        //事件ID号
    char                  tag_name[48];     //事件名称
    char                  tag_ele[128];     //事件元素
} EventTag;
```

使用结构体数组存放读取到的事件格式数据：

```c
typedef struct {
    unsigned int          tag_num;          //事件数量
    EventTag*             tag_array;        //事件数组首地址
} EventTagArray;
```

#### 4.2.3 添加日志事件元素结构体（EventTagEle）并使用二级数组

结构体EventTag具体内容：

```c
typedef struct {
    unsigned int          tag_index;        //事件ID号
    char                  tag_name[48];     //事件名称
    unsigned int          tag_ele_num;      //事件元素个数
    EventTagEle*          tag_ele;          //事件元素数组首地址
} EventTag;
```

结构体EventTagEle具体内容：

```c
typedef struct {
    unsigned int          type;             //事件元素类型  
                                            //1：long 2：double 3：string
    unsigned int          len;              //事件元素长度  len = bits / 8
    char                  name[32];         //事件元素名称
} EventTagEle;
```

二级数组结构体EventTagArray具体内容：

```c
typedef struct {
    int                   num_arrays;        //事件数组数
    int*                  array_lens;        //事件二级数组长度
    EventTag**            tag_array;         //事件一级数组首地址
} EventTagArray;
```

#### 4.2.4 使用特定数组长度并使用位图（Bitmap）记录事件ID号合法性

确定二维数组长度为100：

```c
#define EVENT_TAG_ARRAY_LEN 100;
```

定长位图Bitmap结构体具体内容：

```c
typedef struct {
    uint8_t               bits[EVENT_TAG_ARRAY_LEN/8 + 1];  
} Bitmap;
```

更改后二级数组结构体EventTagArray具体内容：

```c
typedef struct {
    unsigned int          num_arrays;        //事件数组数
    EventTag**            tag_array;         //事件一级数组首地址
} EventTagArray;
```

#### 4.2.5 设计共享内存指针（HiklogSharedTag）

共享内存指针HiklogSharedTag结构体具体内容：

```c
typedef struct HiklogSharedTag {
    int                   magic;             //共享内存校验数
    Bitmap*               bitmap;            //位图数组首地址
    EventTagArray*        eventtag;          //事件二级数组
} HiklogSharedTag;
```

将读出的日志事件格式数据写入共享文件，便于各进程使用：

```c
#define EVENTTAG_FILENAME "/tmp/__eventtagdata__"
HiklogSharedTag* __eventtag_data__;
```

#### 4.2.7 增加共享文件初始化状态变量（HiklogSharedTagState）

共享文件初始化状态变量HiklogSharedTagState枚举类型具体内容：

```c
typedef enum {
    INITIALIZING,                             //初始化中
    INITIALIZED,                              //初始化完成
} HiklogSharedTagState;
```

更改后的共享内存指针HiklogSharedTag结构体具体内容：

```c
typedef struct HiklogSharedTag {
    int                   magic;             //共享内存校验数
    HiklogSharedTagState  state;             //共享文件初始化状态
    Bitmap*               bitmap;            //位图数组首地址
    EventTagArray*        eventtag;          //事件二级数组
} HiklogSharedTag;
```

#### 4.2.8 修改所有存储的地址为相对于首地址的偏移量

最终日志事件存储数据结构为：

```c
#define EVENT_TAG_ARRAY_LEN 100
#define EVENTTAG_FILENAME "/tmp/__eventtagdata__"
#define SHARED_TAG_MAGIC "HikLog77"

/*
 * Bitmap
 */
typedef struct {
    uint8_t               bits[EVENT_TAG_ARRAY_LEN/8 + 1];  
} Bitmap;

#pragma pack(1)
/*
 * EventTagEle
 */
typedef struct {
    unsigned int          type : 2;
    unsigned int          len : 6;
    char                  name[32]; 
} EventTagEle;

/*
 * EventTag
 */
typedef struct {
    unsigned int          tag_index : 12;
    unsigned int          tag_ele_num : 4;
    int                   tag_ele_offset;    //该事件元素首地址偏移量
    char                  tag_name[48];
} EventTag;
#pragma pack()

/*
 * HiklogSharedTagState
 */
typedef enum {
    INITIALIZING,
    INITIALIZED,
} HiklogSharedTagState;

/*
 * HiklogSharedTag
 */
typedef struct HiklogSharedTag {
    char                  magic[8];
    HiklogSharedTagState  state;
    unsigned int          num_arrays;
    int                   bitmap_offset;     //位图首地址偏移量
    int                   eventtag_offset;   //事件首地址偏移量
} HiklogSharedTag;
```

### 4.3 日志事件格式文件读取过程迭代

#### 4.3.1 使用python脚本读取并生成读写操作的头文件和c文件

使用re包中的complie、match、findall函数逐行读取格式文件的数据：

```python
# 定义事件日志标签的正则表达式
    EVENT_LOG_TAG_REGEX = re.compile(r'^(?P<tag_id>\d+) (?P<tag_name>\b\S+?) ')
    EVENT_LOG_ELE_REGEX = re.compile(r'\((.*?)\)')

with open(inputfile, 'r') as file:
    for line in file:
        tag = EVENT_LOG_TAG_REGEX.match(line.strip())
        ele = EVENT_LOG_ELE_REGEX.findall(line.strip())
```

生成包含读出的事件结构体与读写函数声明的头文件：

```c
struct EVENT_1 {
    long ELE_1 : 2;
    double ELE_2;
    char ELE_3[6];
}
// EVENT_2
// EVENT_3
int hiklogbin_write(HikLogger* logger,long timestamp,int tagid,int icnt,...);
int hiklogbin_read(FILE* fp);
```

生成实现读写函数的c文件：

```txt
|--build
     |--scripts
           |--hikbinlog.py
|--output
     |--hiklog_binlog.h
     |--hiklog_binlog.c
```

#### 4.3.2 使用c文件（hiklog_binlog_init.c）读取并初始化事件数组

打开日志格式文件并映射虚拟内存进行读取：

```c
/* 用于映射日志格式文件的结构体 */
typedef struct {
    void*           map_addr;
    size_t          map_len;
} EventTagMap;

EventTagMap* newTagMap;
off_t end;
int fd = -1;
newTagMap = calloc(1, sizeof(EventTagMap));
/* 打开日志格式文件 */
fd = open(fileName, O_RDONLY);
if (fd < 0) {
    fprintf(stderr, "%s: unable to open map '%s': %s\n",
        OUT_TAG, fileName, strerror(errno));
        goto fail;
    }
end = lseek(fd, 0L, SEEK_END);
(void) lseek(fd, 0L, SEEK_SET);
if (end < 0) {
    fprintf(stderr, "%s: unable to seek map '%s'\n", OUT_TAG, fileName);
    goto fail;
}
/* 映射到内存 */
newTagMap->map_addr = mmap(NULL, end, PROT_READ | PROT_WRITE, MAP_PRIVATE,fd, 0);
if (newTagMap->map_addr == MAP_FAILED) {
    fprintf(stderr, "%s: mmap(%s) failed: %s\n",OUT_TAG, fileName, strerror(errno));
    goto fail;
}
newTagMap->map_len = end;
```

逐行读取日志格式文件并初始化日志事件数组

#### 4.3.3 使用c文件（hiklog_binlog_preinit.c）在初始化前预处理

遍历eventlogst文件夹下所有文件：

```c
char filepath[128];
struct dirent *dp;
DIR *dir = opendir(dirname);
struct stat buf; 
int result;  
while ((dp = readdir(dir)) != NULL)
{
    if (strcmp(dp->d_name, ".") != 0 && strcmp(dp->d_name, "..") != 0)
    {
        strcpy(filepath, dirname);             
        strcat(filepath, "/");
        strcat(filepath, dp->d_name);
        result = stat(filepath, &buf);
        if(result == 0 && !(S_IFDIR & buf.st_mode) && file_map(map, filepath)){ 
            //获得初始化所需的数据
            //1.二级数组个数num_arrays（所有事件中ID号最大值/100 + 1）
            //2.位图数组生成（将读取到事件ID号的相应位置为1）
        }
    }
}
closedir(dir);
```

在hiklog_binlog_init.c中可通过preinit得到的数据进行内存一次性分配：

```c
EventTag** eventtag = (EventTag**)malloc(sizeof(EventTag*) * num_arrays);
for(int i = 0;i < num_arrays;i++){
    eventtag[i] = (EventTag*)malloc(sizeof(EventTag) * bitmap_get_max(&bitmap[i]));
}
```

读取日志格式文件并初始化日志事件数组

#### 4.3.4 将初始化的日志事件格式数据写入共享内存并通过指针（HiklogSharedTag）访问

内存分配函数（hiklog_malloc）重写：

```c
void *HIKLOG_MALLOC_START = NULL;                     //映射共享内存起始地址
int HIKLOG_MALLOC_OFFSET = 0;                         //已用内存偏移量
void* hiklog_malloc(size_t size)
{
    void *addr = NULL;
    if(HIKLOG_MALLOC_OFFSET + size >= HIKLOG_SHAREDMEM) {
        return NULL;
    }
    addr = HIKLOG_MALLOC_START + HIKLOG_MALLOC_OFFSET;
    HIKLOG_MALLOC_OFFSET += size;
    return addr;
}
```

通过preinit得到的数据计算共享内存大小：

```c
static size_t sharedmem_size_get(int nummaps,int numeles,int arraylens)
{
    size_t sharedmem = 4;
    sharedmem += arraylens * sizeof(EventTag);        //所有日志事件
    sharedmem += nummaps * sizeof(Bitmap);            //所有位图
    sharedmem += numeles * sizeof(EventTagEle);       //所有事件元素
    sharedmem += sizeof(HiklogSharedTag);             //共享内存指针
    return sharedmem;
}
```

在指定位置打开共享文件并映射内存：

```c
#define EVENTTAG_FILENAME "/tmp/__eventtagdata__"     //共享文件位置
size_t HIKLOG_SHAREDMEM = 0;                          //共享内存大小
HiklogSharedTag* __eventtag_data__ = NULL;            //共享内存指针

int fd;
int ret;
/* 打开共享文件 */
fd = open(EVENTTAG_FILENAME, ORDWR | O_CREAT | O_NOFOLLOW | O_CLOEXEC | O_EXCL);
if (fd < 0) {
    if (errno == EACCES) {
        abort();
    }
    return -1;
}
ret = fcntl(fd, F_SETFD, FD_CLOEXEC);
if (ret < 0)
    goto out;
/* 分配共享文件大小 */
if (ftruncate(fd, HIKLOG_SHAREDMEM) < 0)
    goto out;
/* 映射文件到内存中 */
HIKLOG_MALLOC_START = mmap(NULL, HIKLOG_SHAREDMEM, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
if(HIKLOG_MALLOC_START == MAP_FAILED)
    goto out;
/* 内存初始化 */
memset(HIKLOG_MALLOC_START, 0, HIKLOG_SHAREDMEM);
/* 给共享内存指针分配空间 */
HiklogSharedTag* sharedtag = (HiklogSharedTag*)hiklog_malloc(sizeof(HiklogSharedTag));
//初始化共享内存指针
//1.获得指向位图数组的首地址
//2.获得指向事件二维数组的地址
__eventtag_data__ = sharedtag;                         //赋值给全局遍量
```

读取日志格式文件并将初始化的日志事件数组数据写入共享文件：

```c
EventTag** eventtag = (EventTag**)hiklog_malloc(sizeof(EventTag*) * num_arrays);
for(int i = 0;i < num_arrays;i++){
    eventtag[i] = (EventTag*)hiklog_malloc(sizeof(EventTag) * bitmap_get_max(&bitmap[i]));
}
```

访问时使用HiklogSharedTag进行访问：

```c
EventTagArray* tagarray = __eventtag_data__->eventtag;
Bitmap* bitmap = __eventtag_data__->bitmap;
```

#### 4.3.5 修改HiklogSharedTag地址访问为首地址+偏移访问

地址访问缺陷：

```txt
每个进程将共享文件映射到内存时映射的首地址不同，记录的位图数组首地址与事件二维数组地址
第二次被映射时将会失效；为防止多个进程同时对共享文件进行修改，重置地址的方式不可行。
```

分配内存前先获得当前偏移量：

```c
int hiklog_get_offset()
{
    return HIKLOG_MALLOC_OFFSET;
}
```

将原数据结构中存储指针的结构全部换成偏移量：

```c
/* 原EventTag */
// typedef struct {
//     unsigned int          tag_index;        //事件ID号
//     char                  tag_name[48];     //事件名称
//     unsigned int          tag_ele_num;      //事件元素个数
//     EventTagEle*          tag_ele;          //事件元素数组首地址
// } EventTag;

/* 更新后EventTag */ 
typedef struct {
    unsigned int          tag_index : 12;
    unsigned int          tag_ele_num : 4;
    int                   tag_ele_offset;      //该事件元素首地址偏移量
    char                  tag_name[48];
} EventTag;

/* 原HiklogSharedTag */
// typedef struct {
//     int                   magic;            //共享内存校验数
//     HiklogSharedTagState  state;            //共享文件初始化状态
//     Bitmap*               bitmap;           //位图数组首地址
//     EventTagArray*        eventtag;         //事件二级数组
// } HiklogSharedTag;

/* 更新后HiklogSharedTag */
typedef struct {
    char                  magic[8];
    HiklogSharedTagState  state;
    unsigned int          num_arrays;
    int                   bitmap_offset;       //位图首地址偏移量
    int                   eventtag_offset;     //事件首地址偏移量
} HiklogSharedTag;
```

#### 4.3.6 共享文件初始化前检查是否已经初始化

检查共享文件是否存在：

```c
if(access(EVENTTAG_FILENAME, F_OK) == 0)  //判断文件是否存在
```

判断是否初始化完毕：

```c
do {
    /* 打开共享文件 */
    fd = open(EVENTTAG_FILENAME, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0) {
    return -1;
    }
    struct stat fd_stat;
    if (fstat(fd, &fd_stat) < 0) {
        goto out;
    }
    if ((fd_stat.st_uid != 0)
            || (fd_stat.st_gid != 0)
            || ((fd_stat.st_mode & (S_IWGRP | S_IWOTH)) != 0)
            || (fd_stat.st_size < sizeof(HiklogSharedTag)) ) {
        goto out;
    }
    /* 获取文件大小并映射到内存 */
    HIKLOG_SHAREDMEM = fd_stat.st_size;
    sharedtag = mmap(NULL, HIKLOG_SHAREDMEM, PROT_READ, MAP_SHARED, fd, 0);
    if (sharedtag == MAP_FAILED || strncmp(sharedtag->magic,SHARED_TAG_MAGIC,8) != 0) {
        goto out;
    }
    /* 判断是否初始化完毕 */        
    if (sharedtag->state == INITIALIZING) {         
        munmap(sharedtag, HIKLOG_SHAREDMEM);
        sleep(10000);        //等待初始化
        t++;                 //防止超时等待
    }
    else {
        t = 4;
    }
    close(fd);
} while(t < 3);
```

若初始化完毕则直接使用HiklogSharedTag访问：

```c
__eventtag_data__ = sharedtag;
```

### 4.4 日志事件查找函数迭代

#### 4.4.1 根据事件ID号直接访问相应结构体

使用switch语句找到相应结构体并对其进行读写操作：

```c
switch(tagid)
{
    case : id_1
        struct EVENT_1 event;
        //写入操作
        //读出操作
    case : id_2
        struct EVENT_2 event;
        //写入操作
        //读出操作
    ...
}
```

#### 4.4.2 对日志事件数组通过ID号进行排序并通过二分法查找

对读出的日志事件结构体数组排序：

```c
/* 比较函数重载 */
static int compareEventTags(const void* v1, const void* v2)
{
    const EventTag* tag1 = (const EventTag*) v1;
    const EventTag* tag2 = (const EventTag*) v2;
    return tag1->tagIndex - tag2->tagIndex;
}
qsort(tagarray, numtags, sizeof(EventTag), compareEventTags);
```

根据ID号二分查找相应日志事件格式数据：

```c
const char* hiklog_eventtag_lookup(const EventTag* tagarray, int tagid)
{
    int hi, lo, mid;

    lo = 0;
    hi = map->numTags-1;

    while (lo <= hi) {
        int cmp;

        mid = (lo+hi)/2;
        cmp = tagarray[mid].tag_index - tag;
        if (cmp < 0) {
            /* tag is bigger */
            lo = mid + 1;
        } else if (cmp > 0) {
            /* tag is smaller */
            hi = mid - 1;
        } else {
            /* found */
            return tagarray[mid].tag_ele;
        }
    }

    return NULL;
}
```

#### 4.4.3 通过ID号结合二维数组直接查找

确定二级数组长度便可通过事件ID号确定事件在数组中的位置：

```c
tagNum = tag % EVENT_TAG_ARRAY_LEN;
arrNum = tag / EVENT_TAG_ARRAY_LEN;
/* 判断事件ID是否合法 */
if(tagNum >= tagarray->array_lens[arrNum] || arrNum > tagarray->num_arrays)
{
    return NULL;
}
return tagarray->eventtag[arrNum][tagNum];
```

#### 4.4.4 通过ID号结合偏移量直接查找

通过事件ID号可以确定事件在共享内存中的偏移量：

```c
/* 拿到映射内存首地址 */
void* start = __eventtag_data__;
/* 拿到位图数组首地址（判断事件ID是否合法） */
Bitmap* bitmap = start + __eventtag_data__->bitmap_offset;
/* 判断事件ID是否合法 */
if (arrNum >= __eventtag_data__->num_arrays 
        || tagNum > bitmap_get_max(&bitmap[arrNum]) 
        || bitmap_get(&bitmap[arrNum], tagNum) != 1) {
    return NULL;
} 
else {
    int arrayoffset;          //数组数偏移
    /* 计算偏移量 */
    for(int i = 0;i < arrNum;i++) {
        arrayoffset += sizeof(EventTag)*bitmap_get_max(&bitmap[i]);
    }
    /* 通过偏移量拿到相应事件格式数据 */
    EventTag* tag = start + __eventtag_data__->eventtag_offset //第一个事件
                          + arrayoffset + tagNum * sizeof(EventTag);
    /* 判断输入数据是否与事件元素数相等 */
    if(tag->tag_ele_num != icnt && icnt != -1){
        return NULL;
    }
    else {
        return tag;
    }
}
```

### 4.5 二进制日志读写过程迭代

#### 4.5.1 根据python脚本读取的日志格式数据生成相应的读写函数

为每一个事件生成相应结构体并生成头文件：

```c
struct EVENT_NAME {
	type ele_1 : n1;
	type ele_2 : n2;
}
...
```

通过事件ID号与switch-case语句进行相应的读写操作：

```c
/* 接收可变参数 */
int hiklog_binlog_write(HikLogger* logger,int tagid,long timestamp,int icnt,...) 
{
    va_list args;
    va_start(args,icnt);
	switch(tagid)
	{
    	case : id_1
        	struct EVENT_1 event;
        	/* 判断输入事件元素数是否合法 */
        	if(event.tag_ele_num != icnt){
            	printf("Invalid Input!\n");
                break;
        	}
            /* 将输入数据赋值给事件结构体 */
            event.ele_1 = va_arg(args,type);
            event.ele_2 = va_arg(args,type);
            ...
            /* 将日志结构体数据二进制写入 */
            hiklog_file_write(logger, (const char *)&event, sizeof(EVENT_1));
    	case : id_2
        	struct EVENT_2 event;
            //如上
    	...
        default:
            break;
	}
    printf("Log Written!\n");
}
/* 读取二进制日志文件 */
int hiklog_binlog_read(FILE* fp)
{
    int ret = 1 ;
    int tagid = 0;
    fseek(fp, 0L, SEEK_SET);
    while(ret > 0){
        ret = hiklog_file_read(fp , &tagid , 4);
        if(ret != 4) return -1;
        switch(tagid)
		{
    		case : id_1
        		struct EVENT_1 event;
            	/* 读出日志结构体数据 */
            	ret = hiklog_file_read(fp , &event , sizeof(EVENT_1));
                if(ret != sizeof(EVENT_1)) return -1;
            	...
            	//按格式输出日志数据
    		case : id_2
        		struct EVENT_2 event;
            	//如上
    		...
        	default:
            	break;
		}
    }
}   
```

弊端：

```txt
当日志事件种类变多时，头文件中的结构体定义与c文件中的switch-case语句将变得越来越长，处理效率降低。
```

#### 4.5.2 根据事件ID号在事件格式结构体数组中找到相应格式并进行读写

日志写入函数改进：

```c
/* 找到相应格式结构体 */
EventTag* tag = hiklog_eventtag_lookup(tagid,icnt);
if(tag == NULL){
    printf("Invalid input!\n");
    return 0;
}
/* 找到事件元素格式首地址 */
EventTagEle* tagele =  start + tag->tag_ele_offset;
/* 遍历所有元素并写入数据 */
for(int i = 0;i < icnt;i++){
    if(tagele[i].type == 1){
        long tmp = va_arg(args,long);
        hiklog_file_write(logger, (const char *)&tmp, tagele[i].len);
    }
    else if(tagele[i].type == 2){
        double tmp = va_arg(args,double);
        hiklog_file_write(logger, (const char *)&tmp, tagele[i].len);
    }
    else if(tagele[i].type == 3){
        char tmp[tagele[i].len];
        strcpy(tmp, va_arg(args,char*));
        hiklog_file_write(logger, (const char *)&tmp, tagele[i].len);
    }
}
printf("Log written!\n");
```

日志读出函数改进：

```c
int ret = 1 ;
int tagid = 0;
fseek(fp, 0L, SEEK_SET);
while(ret > 0){
    /* 读出事件ID号 */
    ret = hiklog_file_read(fp , &tagid , 4);
    if(ret != 4) return -1;
    char c[8];
    sprintf(c, "%d", tagid);
    void* start = __eventtag_data__;
    /* 根据ID号找到格式数据 */
    EventTag* tag = hiklog_eventtag_lookup(tagid,-1);
    /* 找到事件元素格式首地址 */
    EventTagEle* tagele =  start + tag->tag_ele_offset;
    /* 输出字符串初始化 */
    char op[128];
    memset(op, 0, sizeof(op));
    long timestamp = 0;
    /* 读出时间戳 */
    ret = hiklog_file_read(fp , &timestamp , 4);
    if(ret != 4) return -1;
    time_t t = timestamp;
    /* 将时间从long类型转换为输出格式的字符串 */
    struct tm *tmp_time = localtime(&t);
    char ts[100];
    strftime(ts, sizeof(ts), "[%04Y-%02m-%02d %H:%M:%S", tmp_time);
    sprintf(op, "%s%s ",op, ts);    
    sprintf(op, "%s tid:%s] ",op, c);   
    sprintf(op, "%s%s ",op, tag->tag_name);
    /* 遍历所有元素并读出数据 */
	for(int j = 0;j < tag->tag_ele_num;j++){
        sprintf(op, "%s(%s",op, tagele[j].name);
        if(tagele[j].type == 1){
            long tmp = 0;
            ret = hiklog_file_read(fp , &tmp , tagele[j].len);
            if(ret != tagele[j].len) return -1;
            char t[8];
            sprintf(t, "%ld)", tmp);
            sprintf(op, "%s%s ",op, t);
        }
        else if(tagele[j].type == 2){
            double tmp = 0;
            char t[8];
            ret = hiklog_file_read(fp , &tmp , tagele[j].len);
            if(ret != tagele[j].len) return -1;
            sprintf(t,"%.2lf)",tmp);
            sprintf(op, "%s%s ",op, t);
        }
        else if(tagele[j].type == 3){
            char tmp[tagele[j].len];
            ret = hiklog_file_read(fp , &tmp , tagele[j].len);
            if(ret != tagele[j].len) return -1;
            sprintf(op, "%s\"%s\") ",op, tmp);
        }
    }
    int i = strlen(op);
    op[i++] = '\n';
    printf("%s",op);
    /* 清空输出数组便于读出下一条记录 */
    memset(op, 0, sizeof(op));
}

```

## 五、测试效果

### 5.1 日志格式测试数据

eventlogst_1.txt

```txt
1 power_sleep_requested (wakeLocksCleared|L|16)
2 power_screen_broadcast_send (wakelockCount|L|16)
3 power_screen_broadcast_done (on|L|16),(broadcastDuration|D|64),(wakelockCount|L|16)
4 power_screen_broadcast_stop (which|L|16),(wakelockCount|L|16)
401 am_service_crashed_too_much (User|L|16),(CrashCount|L|16),(ComponentName|S|40),(PID|L|16)
403 am_schedule_service_restart (User|L|16),(ComponentName|S|40),(Time|D|64)
411 am_provider_lost_process (User|L|16),(PackageName|S|40),(UID|L|16),(Name|S|40)
413 am_process_start_timeout (User|L|16),(PID|L|16),(UID|L|16),(ProcessName|S|40)
```

eventlogst_2.txt

```txt
104 view_build_drawing_cache (Viewcreateddrawingcache|D|64)
105 view_use_drawing_cache (Viewdrawnusingbitmapcache|L|16)
201 exp_det_cert_pin_failure (certs|S|96)
202 snet_event_log (subtag|S|16),(uid|L|16),(message|S|128)
505 battery_level (level|L|16),(voltage|L|8),(temperature|L|8)
555 battery_status (status|L|16),(health|L|16),(present|L|40),(plugged|L|40),(technology|S|48)
```

eventlogst_3.txt

```txt
5 power_screen_state (offOrOn|L|16),(becauseOfUser|L|16),(totalTouchDownTime|D|64),(touchCycles|L|16)
6 power_partial_wake_state (releasedorAcquired|L|16),(tag|S|64)
101 http_stats (useragent|S|48),(response|D|64),(processing|D|64),(tx|L|24),(rx|L|24)
102 viewroot_draw (Drawtime|L|16)
103 viewroot_layout (Layouttime|L|24)
520 my_internship (Ename|S|72),(Startdate|L|16),(Enddate|L|16),(Work|S|80)
```

### 5.2 日志输入测试数据

二进制写入测试函数：

```c
int hiklogbin_write_test(HikLogger* logger)
{
    hiklog_binlog_write(logger,1,1720000000,1,10);
    hiklog_binlog_write(logger,3,1720000000,3,4,3.3,2);
    hiklog_binlog_write(logger,2,1720000000,1,1);
    hiklog_binlog_write(logger,6,1720000003,2,27,"hiklogt");    
    hiklog_binlog_write(logger,8,1720000000,3,4,3.3,2);
    hiklog_binlog_write(logger,5,1720080002,4,5,6,7.7,8);
    hiklog_binlog_write(logger,4,1720080001,3,2,3);
    hiklog_binlog_write(logger,104,1720080104,1,10.4);
    hiklog_binlog_write(logger,107,1720080001,2,107);
    hiklog_binlog_write(logger,101,1720080101,5,"testn",10.1,1.01,101,1001);
    hiklog_binlog_write(logger,103,1720083301,1,103);
    hiklog_binlog_write(logger,105,1720085501,1,105);
    hiklog_binlog_write(logger,201,1720102010,1,"HIKBINLOGTS");
    hiklog_binlog_write(logger,202,1720222202,3,"H",202,"this is hiklog!");
    hiklog_binlog_write(logger,303,1720085501,2,105,3.3);
    hiklog_binlog_write(logger,408,1720085501,2,408,408);
    hiklog_binlog_write(logger,411,1730085501,4,411,"skyy",114,"dzyy");
    hiklog_binlog_write(logger,554,1720185555,5,5,55,55,5,"enjoy");
    hiklog_binlog_write(logger,413,1720085413,4,413,4,3,"read");
    hiklog_binlog_write(logger,520,1724665413,4,"DengZeyu",703,830,"hikbinlog");
    return 0;
}
```

### 5.3 测试结果

二进制写入测试结果：

```shell
[root@VEXPRESS /mnt/9p]# ./hiklog -x
Log written!
Log written!
Log written!
Log written!
Invalid input!
Log written!
Invalid input!
Log written!
Invalid input!
Log written!
Log written!
Log written!
Log written!
Log written!
Invalid input!
Invalid input!
Log written!
Invalid input!
Log written!
Log written!
```

读出日志并显示测试结果：

```shell
[root@VEXPRESS /mnt/9p]# ./hiklog -b ../hikramfs/hiklogbin.0
[2024-07-03 09:46:40  tid:1] power_sleep_requested  (wakeLocksCleared|10)
[2024-07-03 09:46:40  tid:3] power_screen_broadcast_done  (on|4) (broadcastDuration|3.30) (wakelockCount|2)
[2024-07-03 09:46:40  tid:2] power_screen_broadcast_send  (wakelockCount|1)
[2024-07-03 09:46:43  tid:6] power_partial_wake_state  (releasedorAcquired|27) (tag|"hiklogt")
[2024-07-04 08:00:02  tid:5] power_screen_state  (offOrOn|5) (becauseOfUser|6) (totalTouchDownTime|7.70) (touchCycles|8)
[2024-07-04 08:01:44  tid:104] view_build_drawing_cache  (Viewcreateddrawingcache|10.40)
[2024-07-04 08:01:41  tid:101] http_stats  (useragent|"testn") (response|10.10) (processing|1.01) (tx|101) (rx|1001)
[2024-07-04 08:55:01  tid:103] viewroot_layout  (Layouttime|103)
[2024-07-04 09:31:41  tid:105] view_use_drawing_cache  (Viewdrawnusingbitmapcache|105)
[2024-07-04 14:06:50  tid:201] exp_det_cert_pin_failure  (certs|"HIKBINLOGTS")
[2024-07-05 23:30:02  tid:202] snet_event_log  (subtag|"H") (uid|202) (message|"this is hiklog!")
[2024-10-28 03:18:21  tid:411] am_provider_lost_process  (User|411) (PackageName|"skyy") (UID|114) (Name|"dzyy")
[2024-07-04 09:30:13  tid:413] am_process_start_timeout  (User|413) (PID|4) (UID|3) (ProcessName|"read")
[2024-08-26 09:43:33  tid:520] my_internship  (Ename|"DengZeyu") (Startdate|703) (Enddate|830) (Work|"hikbinlog")
```

### 5.4 维护测试

为了方便后续维护，添加了查看当前日志事件格式数组的函数：

```c
int hiklog_eventtag_show()
{
    void* start = __eventtag_data__;
    Bitmap* bitmap = start + __eventtag_data__->bitmap_offset;
    int arrayoffset;
    for(int i = 0;i < __eventtag_data__->num_arrays;i++){
        for(uint8_t j = 0;j < bitmap_get_max(&bitmap[i]);j++){
            /* 判断事件ID号是否合法 */
            if(bitmap_get(&bitmap[i],j+1) == 1){
                arrayoffset = 0;
                for(int l = 0;l < i;l++){
                    arrayoffset += sizeof(EventTag)*bitmap_get_max(&bitmap[l]);
                }
                EventTag* tag = start + __eventtag_data__->eventtag_offset 
                    				  + arrayoffset + j*sizeof(EventTag);
                EventTagEle* tagele =  start + tag->tag_ele_offset;
                printf("%d %s ",tag->tag_index,tag->tag_name);
                for(int k = 0;k < tag->tag_ele_num;k++){
                    printf("(%s",tagele[k].name);
                    if(tagele[k].type == 1){
                        printf("L|%d) ",tagele[k].len);
                    }
                    else if(tagele[k].type == 2){
                        printf("D|%d) ",tagele[k].len);
                    }
                    else if(tagele[k].type == 3){
                        printf("S|%d) ",tagele[k].len);
                    }
                }
                printf("\n");
            }
        }
    }
    return 0;
}

```

输出测试效果：

```shell
[root@VEXPRESS /mnt/9p]# ./hiklog -p
1 power_sleep_requested  (wakeLocksCleared|L|2) 
2 power_screen_broadcast_send  (wakelockCount|L|2)
3 power_screen_broadcast_done  (on|L|2) (broadcastDuration|D|8) (wakelockCount|L|2)
4 power_screen_broadcast_stop  (which|L|2) (wakelockCount|L|2)
5 power_screen_state  (offOrOn|L|2) (becauseOfUser|L|2) (totalTouchDownTime|D|8) (touchCycles|L|2)
6 power_partial_wake_state  (releasedorAcquired|L|2) (tag|S|8)
101 http_stats  (useragent|S|6) (response|D|8) (processing|D|8) (tx|L|3) (rx|L|3)
102 viewroot_draw  (Drawtime|L|2)
103 viewroot_layout  (Layouttime|L|3)
104 view_build_drawing_cache  (Viewcreateddrawingcache|D|8)
105 view_use_drawing_cache  (Viewdrawnusingbitmapcache|L|2)
201 exp_det_cert_pin_failure  (certs|S|12)
202 snet_event_log  (subtag|S|2) (uid|L|2) (message|S|16)
401 am_service_crashed_too_much  (User|L|2) (CrashCount|L|2) (ComponentName|S|5) (PID|L|2)
403 am_schedule_service_restart  (User|L|2) (ComponentName|S|5) (Time|D|8)
411 am_provider_lost_process  (User|L|2) (PackageName|S|5) (UID|L|2) (Name|S|5)
413 am_process_start_timeout  (User|L|2) (PID|L|2) (UID|L|2) (ProcessName|S|5)
505 battery_level  (level|L|2) (voltage|L|1) (temperature|L|1)
520 my_internship  (Ename|S|9) (Startdate|L|2) (Enddate|L|2) (Work|S|10)
555 battery_status  (status|L|2) (health|L|2) (present|L|5) (plugged|L|5) (technology|S|6)
```



