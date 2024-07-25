import bz2
import json
import re
from sre_constants import SUCCESS
from tarfile import XHDTYPE
from time import sleep
from fastapi import FastAPI,File,UploadFile,Form,Body,WebSocket,HTTPException
from fastapi.responses import FileResponse
from sqlalchemy import table
from uvicorn import Config, Server
from sqlmodel import Field, Session, SQLModel, create_engine, select, update,func
from fastapi.middleware.cors import CORSMiddleware
import oracledb
import uvicorn
from datetime import datetime,date
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
import time
import pandas as pd
import polars as pl


#重启oracle服务监听,数据库信息
#lsnrctl status   identified by trff_app
#uvicorn main:app --reload   

#https://www.oracle.com/database/technologies/appdev/python/quickstartpythononprem.html
#https://docs.sqlalchemy.org/en/20/dialects/oracle.html#module-sqlalchemy.dialects.oracle.oracledb
app = FastAPI()


# 添加CORS中间件，允许所有来源、所有方法和所有头
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有源
    allow_credentials=True,  # 允许凭证，如cookies
    allow_methods=["*"],  # 允许所有HTTP方法
    allow_headers=["*"],  # 允许所有请求头
)

engine=create_engine("oracle+oracledb://ksk:ksk@192.168.1.116:1521/?service_name=orcl",echo=True)
engine2=create_engine("sqlite:///database.db",echo=True)
engine3=create_engine("oracle+oracledb://vehweb:vehweb@192.168.1.116:1521/?service_name=orcl",echo=True)

@app.get("/")
def root():
    return {"服务器已启动。"}

class KSK_KCXX(SQLModel, table=True):
    FCODE: str = Field(primary_key=True)
    FNAME: str
    FSBH: str
    KCLB: str
    KCDDDH: str


@app.get("/getKC")
def getKC():
    with Session(engine) as session:
        statement = select(KSK_KCXX).where(KSK_KCXX.KCDDDH != None)
        results = session.exec(statement)
        answer=[]
        for result in results:
            answer.append(result)
    return answer

@app.post("/uploadfile/")
async def create_upload_file(file: UploadFile = File(...), withKey: str = Form(...)):
    """
    上传一个文件，并且携带Key值。

    参数:
    - file: 要上传的文件.
    - key: 携带的key值.

    """
    # 如果不是excel文件，返回错误
    print("key=", withKey)
    if file.content_type != "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
        return {"error": "只能上传Excel文件"}
    # 保存文件
    with open(file.filename, "wb") as f:
        f.write(file.file.read())

    return {"filename": file.filename}


class INFORMATION(SQLModel, table=True):
    INFO: str = Field(primary_key=True)
    
@app.get("/bourdinfo")
async def bourdinfo():
    """
    查询前端的公告信息。

    Returns:
        str: 要给到前端的公告文字。
    """
    with Session(engine2) as session:
        statement = select(INFORMATION)
        results = session.exec(statement)
        answer=[]
        for result in results:
            answer.append(result)
    return answer[0].INFO

@app.post("/bourdinfochange")
async def bourdinfochange(info: str=Body(...)):
    """
    根据提供的信息，修改前端的公告信息。

    Parameters:
    - info (str): 新公告内容

    Returns:
    成功返回成功消息
    """
    with Session(engine2) as session:
        statement = update(INFORMATION).values(INFO=info)
        session.exec(statement)
        session.commit()
        return info


class BlackListLog(SQLModel, table=True):
    sfzmhm: str 
    xm: str
    type: str
    lrr: str
    bz: str
    lrsj: datetime=Field(default=datetime.now(),primary_key=True)

class KSYJ_HMD(SQLModel,table=True):
    sfzmhm: str= Field(primary_key=True)

@app.post("/postblacklist")
async def postblacklist(list: BlackListLog = Body(...)):
    """ 
    录入黑名单列表

    Parameters:
    - list (list): 黑名单列表

    Returns:
    成功返回成功消息
    """
    lrsj = datetime.now()
    list_instance = BlackListLog(sfzmhm=list.sfzmhm, xm=list.xm, type=list.type, lrr=list.lrr, bz=list.bz, lrsj=lrsj)
    hmd_instance = KSYJ_HMD(sfzmhm=list.sfzmhm)
    
    with Session(engine3) as session3:
        statement = select(KSYJ_HMD).where(KSYJ_HMD.sfzmhm == list.sfzmhm)
        results = session3.exec(statement)
        
        if list.type == "1":
            # 如果已经存在该身份证号码
            if results.first() is not None:
                return {"error": "已经存在该身份证号码"}
            
            session3.add(hmd_instance)
            session3.commit()
            with Session(engine2) as session2:
                session2.add(list_instance)
                session2.commit()
                return {"success": {"sfzmhm": list.sfzmhm, "xm": list.xm, "type": list.type, "lrr": list.lrr, "bz": list.bz, "lrsj": lrsj}}
            
        elif list.type == "2":
            # 如果不存在该身份证号码
            if results.first() is None:
                return {"error": "不存在该身份证号码"}
            
            statement_del = select(KSYJ_HMD).where(KSYJ_HMD.sfzmhm == list.sfzmhm)
            result_del = session3.exec(statement_del)
            session3.delete(result_del.one())
            session3.commit()

            with Session(engine2) as session2:
                session2.add(list_instance)
                session2.commit()
                return {"delete": {"sfzmhm": list.sfzmhm, "xm": list.xm, "type": list.type, "lrr": list.lrr, "bz": list.bz, "lrsj": lrsj}}


class KSYJ_ZS_IMPORTDEPT(SQLModel,table=True):
    KCMC: str
    ZDGZ: str
    KM: str
    ZDSJDCRS: str
    ZDQKRS: str
    ZDQKL: str
    ZDHGRS: str
    ZDBHGRS: str
    ZDHGL: str
    YYZRS: str
    DRSJDCRS: str
    DRQKRS: str
    DRQKL: str
    DRHGRS: str
    DRBHGRS: str
    DRHGL: str
    GXSJ: datetime=Field(default=datetime.now(),primary_key=True)
    JLZT:str
    bz:str=Field(default=None)
    KSRQ:date=Field (default=date.today())

@app.post("/postimportdept")
async def postimportdept(list: KSYJ_ZS_IMPORTDEPT=Body(...)):
    gxsj = datetime.now()
    list_instance = KSYJ_ZS_IMPORTDEPT(KCMC=list.KCMC, ZDGZ=list.ZDGZ, KM=list.KM, ZDSJDCRS=list.ZDSJDCRS, ZDQKRS=list.ZDQKRS, ZDQKL=list.ZDQKL, ZDHGRS=list.ZDHGRS, ZDBHGRS=list.ZDBHGRS, ZDHGL=list.ZDHGL, YYZRS=list.YYZRS, DRSJDCRS=list.DRSJDCRS, DRQKRS=list.DRQKRS, DRQKL=list.DRQKL, DRHGRS=list.DRHGRS, DRBHGRS=list.DRBHGRS, DRHGL=list.DRHGL, GXSJ=gxsj, JLZT="1",KSRQ=datetime.strptime(list.KSRQ, "%Y/%m/%d"))
    with Session(engine3) as session3:
        session3.add(list_instance)
        session3.commit()
        return {"success": "已上传成功"}


class KSYJ_YYQK(SQLModel,table=True):
    LSH: str=Field(primary_key=True)
    KSYY: str
    SFZMHM: str
    XM: str
    KSKM:str
    KSCX:str
    FNAME:str
    YKRQ:date
    SSJXDSP:str
    SSFSDSP:str

@app.post("/postyyks")
async def postyyks(list: KSYJ_YYQK=Body(...)):
    list_instance = KSYJ_YYQK(FNAME=list.FNAME, YKRQ=datetime.strptime(list.YKRQ, "%Y/%m/%d"))
    with Session(engine3) as session3:
        subquery = select(KSYJ_HMD.sfzmhm)
        statement = select(func.count(KSYJ_YYQK.SFZMHM)).where(KSYJ_YYQK.SFZMHM.in_(subquery)).where(KSYJ_YYQK.YKRQ==datetime.strptime(list.YKRQ, "%Y/%m/%d")).where(KSYJ_YYQK.FNAME==list.FNAME)
        results=session3.exec(statement).one()
        return {"success": results}

class dateRange(SQLModel):
    startdate: date
    enddate: date
@app.post("/postksyjyyqk")
async def postksyjyyqk(list: dateRange=Body(...)):
    list_instance = dateRange(startdate=list.startdate, enddate=list.enddate)
    with Session(engine3) as session3:
        # 执行 ZS_KSYJ_YYQK 存储过程
        zs_ksyj_yyqk = text(" BEGIN ZS_KSYJ_YYQK(:param1, :param2);end;")
        procedure_params = {
            "param1": list_instance.startdate,  
            "param2": list_instance.enddate
        }
        session3.execute(zs_ksyj_yyqk, params=procedure_params)
        
        statement = select(func.count()).select_from(KSYJ_YYQK)
        results=session3.exec(statement).one()
        return {"success": results}

class KSYJ_YYQK_LOG(SQLModel,table=True):
    id:int=Field(primary_key=True)
    LSH: str
    KSYY: str
    SFZMHM: str
    XM: str
    KSKM:str
    KSCX:str
    FNAME:str
    YKRQ:date
    SSJXDSP:str
    SSFSDSP:str
    gxrq:datetime=Field(default=datetime.now())
    bz:str=Field(default=None)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    results2=[]
    try:
        await websocket.accept()
        while True:
            data = await websocket.receive_json()
            print (data)
            list = dateRange(startdate=data['startdate'], enddate=data['enddate'])
            list_instance = dateRange(startdate=list.startdate, enddate=list.enddate)

            with Session(engine3) as session3:
                # 执行 ZS_KSYJ_YYQK 存储过程
                zs_ksyj_yyqk = text(" BEGIN ZS_KSYJ_YYQK(:param1, :param2);end;")
                procedure_params = {
                    "param1": list_instance.startdate,  
                    "param2": list_instance.enddate
                }
                session3.execute(zs_ksyj_yyqk, params=procedure_params)

                statement = select(func.count()).select_from(KSYJ_YYQK)
                results =  session3.exec(statement).one()
            
                #记录增加日志,把KSYJ_YYQK表的所有新数据插入到KSYJ_YYQK_LOG表
                with Session(engine2) as session2:
                    sentense = session3.execute(text("SELECT * FROM KSYJ_YYQK"))  
                    results2 = sentense.fetchall()
                    for index,result in enumerate(results2):
                        list_instance = KSYJ_YYQK_LOG(id=index+1,LSH=result[0], KSYY=result[1], SFZMHM=result[2], XM=result[3], KSKM=result[4], KSCX=result[5], FNAME=result[6], YKRQ=result[7], SSJXDSP=result[8], SSFSDSP=result[9], gxrq=datetime.now(), bz=None)
                        session2.add(list_instance)
                    session2.commit()
            await websocket.send_json({"success": results})
    except SQLAlchemyError as e:
        #将results2变成json序列返回，方便前端查看
        results2_json = json.dumps(results2, default=str)
        
        await websocket.send_json({"error": str(e.orig), "results2": results2_json})
    except Exception as e:
        await websocket.send_json({"error": str(e)})
    finally:
        await websocket.close()

@app.get("/getyyqksfzmhm")
async def getyyqksfzmhm():
    with Session(engine3) as session3:
        statement = select(KSYJ_YYQK.SFZMHM).distinct()
        results = session3.exec(statement)
        answer=[]
        for result in results:
            answer.append(result)
        pd.DataFrame(answer).to_excel("getyyqksfzmhm.xlsx",'YYQK表导出的身份证号码',header=False,index=False)
    return FileResponse("getyyqksfzmhm.xlsx")

@app.post("/uploadfileFJ")
async def uploadfileFJ(file: UploadFile = File(...)):
    """
    上传飞机航行数据

    Parameters:
    - file (UploadFile): 上传的文件.

    Returns:
    - None
    """
    df=pl.read_excel(file.file.read())
    print(df)
