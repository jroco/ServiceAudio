#!/usr/bin/env python

from flask import Flask, make_response, request
from flask_restful import Api, Resource, reqparse
import cx_Oracle, json, sys, logging, hashlib, os, paramiko

ServiceAudio = Flask(__name__)
api = Api(ServiceAudio)

def ExtractDirFile(InputFilename):
        logging.info('ExtractDirFile: Entering to funtion ExtractDirFile with argumrnt %s', InputFilename)
        FilenameExtract = InputFilename.split("/")
        f = len(FilenameExtract) - 1
        count = 0
        var1 = ''
        while (count < f ):
                logging.debug('ExtractDirFile: The count is:', count, FilenameExtract[count])
                var1 = var1 + FilenameExtract[count] + '/'
                count = count + 1
        return var1,FilenameExtract[f]

def md5Checksum(filePath):
    with open(filePath, 'rb') as fh:
        m = hashlib.md5()
        while True:
            data = fh.read(8192)
            if not data:
                break
            m.update(data)
        return m.hexdigest()

def CopyFile(LocalPath, RemotePath, Ip, HostnameIn):
        StatusCopy = ''
        CopyResult = ''
        logging.info('CopyFile: Entering to function CopyFile with arguments Filename: %s RemotePath: %s Ip: %s', LocalPath, RemotePath, Ip)
        ssh = paramiko.SSHClient()
        ssh.load_host_keys(os.path.expanduser(os.path.join("~", ".ssh", "known_hosts")))
        resultCopyFile = []
        try:
                ssh.connect(Ip, username="XXX", password="XXX")
        except OSError as e:
                logging.info('CopyFile: ERROR OS EXCEPTION %s ',str(e))
                StatusCopy = 'ERROR'
                CopyResult = str(e)
                return ({'STATUSCOPY': StatusCopy, 'FILENAMEREMOTE': RemotePath, 'FILENAMELOCAL': LocalPath, 'COPYRESULT': CopyResult, 'HOSTNAME': HostnameIn, 'IP': Ip })
                pass
                sftp.close()
                ssh.close()
        except IOError as e:
                logging.info('CopyFile: ERROR IOERROR %s ',str(e))
                StatusCopy = 'ERROR'
                CopyResult = str(e)
                return ({'STATUSCOPY': StatusCopy, 'FILENAMEREMOTE': RemotePath, 'FILENAMELOCAL': LocalPath, 'COPYRESULT': CopyResult, 'HOSTNAME': HostnameIn, 'IP': Ip })
                pass
        except Exception as e:
                logging.info('CopyFile: ERROR OTHER EXCEPTION %s ',str(e))
                pass
        sftp = ssh.open_sftp()
        logging.info('CopyFile: Loged Success into %s ', Ip)
        md5file1 = md5Checksum(LocalPath)
        remote_path, file_remote = ExtractDirFile(RemotePath)
        try:
                sftp.chdir(remote_path)  #Test if remote_path exists
        except IOError:
                logging.info('CopyFile: Remote folder not exist, will proceed to create it: %s',remote_path)
                sftp.mkdir(remote_path)  #Create remote_path
                sftp.chdir(remote_path)
        except Exception as e:
                logging.info('CopyFile: WARN OTHER EXCEPTION',str(e))
                pass

        LocalPathChk = LocalPath + ".chk"
        sftp.put(LocalPath,RemotePath)
        sftp.get(RemotePath,LocalPathChk)
        md5file2 = md5Checksum(LocalPathChk)
        logging.info('CopyFile: MD5 OUT: %s MD5IN: %s',md5file1,md5file2)
        if md5file1 == md5file2:
                StatusCopy = 'OK'
                CopyResult = md5file1
        else:
                StatusCopy = 'ERROR'
                CopyResult = 'MD5 Check Error'
                return ({'STATUSCOPY': StatusCopy, 'FILENAMEREMOTE': RemotePath, 'FILENAMELOCAL': LocalPath, 'COPYRESULT': CopyResult, 'HOSTNAME': HostnameIn, 'IP': Ip })
        sftp.close()
        ssh.close()
        return ({'STATUSCOPY': StatusCopy, 'FILENAMEREMOTE': RemotePath, 'FILENAMELOCAL': LocalPath, 'COPYRESULT': CopyResult, 'HOSTNAME': HostnameIn, 'IP': Ip })

def AskHostServ(CS,VDN):
        con = cx_Oracle.connect('user/passwd@SCHEMA')
        logging.info('AskDbCS: DB Query routine with CS: %s and VDN: %s',CS, VDN)
        result = []
        resultfrmt = []
        cursor = con.cursor()
        query1="select HOSTNAME,IP from IVR.TB_ASTERISK_SIP where HOSTNAME in (select HOSTNAME from IVR.TB_ASTERISK_SERVICIO where ASTGROUP = (select ASTGROUP from IVR.TB_SERVICIO_INFO where CS = 'XXXX' and VDN = 'YYYY' )) and STATUS = 'OK' order by HOSTNAME asc"
        query2 = query1.replace("XXXX",CS)
        query3 = query2.replace("YYYY",VDN)
        logging.debug('AskDbCS: Query2 %s', query3)
        try:
            cursor.execute(query3)
        except cx_Oracle.DatabaseError as e:
                error, = e.args
                if error.code == 955:
                        logging.info('AskHostServ: Table already exists')
                if error.code == 1031:
                        logging.info('AskHostServ: Insufficient privileges - are you sure you are using the owner account?')
                logging.info('AskHostServ: ORACLE ERROR %s %s %s',error.code,error.message,error.context)
                return error.message, 500

        for row in cursor.fetchall():
                result.append(row)
        con.close()
        for row in result:
                resultfrmt.append({'HOSTNAME': row[0], 'IP': row[1]})
        return resultfrmt

def AskDbCS(CS):
        con = cx_Oracle.connect('user/passwd@SCHEMA')
        logging.info('AskDbCS: DB Query routine with CS: %s ',CS)
        result = []
        resultfrmt = []
        cursor = con.cursor()
        try:
            cursor.execute("select CS,VDN,SERVICIO_CUST from IVR.TB_SERVICIO_INFO where CS = :var1 order by VDN asc", var1=CS)
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if error.code == 955:
                logging.info('AskDbServ: Table already exists')
            if error.code == 1031:
                logging.info('AskDbServ: Insufficient privileges - are you sure you are using the owner account?')
            logging.info('AskDbCS: ORACLE ERROR %s %s %s',error.code,error.message,error.context)
            return error.message, 500

        for row in cursor.fetchall():
                result.append(row)
        con.close()
        for row in result:
                resultfrmt.append({'CS': row[0], 'VDN': row[1], 'SERVICIO': row[2]})
        return resultfrmt

def AskDbServ(CS,VDN):
        con = cx_Oracle.connect('user/passwd@SCHEMA')
        logging.info('AskDbServ: DB Query routine with CS: %s VDN: %s',CS,VDN)
        result = []
        resultfrmt = []
        cursor = con.cursor()
        try:
            cursor.execute("SELECT * from IVR.TB_IVRDYN_DESC where CS = :var1 and VDN = :var2 order by ID asc", var1=CS, var2=VDN)
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if error.code == 955:
                logging.info('AskDbServ: Table already exists')
            if error.code == 1031:
                logging.info('AskDbServ: Insufficient privileges - are you sure you are using the owner account?')
            logging.info('AskDbServ: ORACLE ERROR %s %s %s',error.code,error.message,error.context)
            return error.message, 500

        for row in cursor.fetchall():
                result.append(row)
        con.close()
        for row in result:
                resultfrmt.append({'ID': row[0], 'CS': row[1], 'VDN': row[2], 'BRANCH': row[3], 'FLOWTYPE': row[4], 'STD_DESC': row[5], 'CUST_DESC': row[6], 'FILENAME': row[7]})
        return resultfrmt

class Distributor(Resource):
        def put(self):
                parser = reqparse.RequestParser()
                parser.add_argument("CS")
                parser.add_argument("VDN")
                parser.add_argument("BRANCH")
                parser.add_argument("FILENAME")
                parser.add_argument("FLOWTYPE")
                args = parser.parse_args()
                try:
                        if args["CS"] and args["VDN"] and args["BRANCH"] and args["FILENAME"] and args["FLOWTYPE"]:
                                CsIn = args["CS"].encode('utf8')
                                VdnIn = args["VDN"].encode('utf8')
                                BranchIn = args["BRANCH"].encode('utf8')
                                FilenameIn = args["FILENAME"].encode('utf8')
                                FlowtypeIn = args["FLOWTYPE"].encode('utf8')
                                logging.info('Distributor: Input data variables CS: %s VDN: %s BRANCH: %s FILENAME: %s FLOWTYPE: %s',CsIn,VdnIn,BranchIn,FilenameIn,FlowtypeIn)
                        else:
                                resp = make_response("Insuficient data argument to process \n")
                                resp.headers['WARNING'] = 'Insuficient data argument to process'
                                resp.status_code = 418
                                return resp
                except (ValueError, KeyError, TypeError):
                        logging.info('Distributor: Data Insuficient to process exception')
                        return "Data Insuficient to process exception", 418
                resp = []
                #print(request.remote_addr)
                FullFilenameIn = '/home/prm_user/' + CsIn + VdnIn + '/' + FilenameIn
                FullFilenameOut = '/home/prm_user/' + CsIn + VdnIn + '/' + FilenameIn
                logging.info('Distributor: File to check: %s',FullFilenameIn)
                # Rutina de ir a buscar el audio (validacion si existe)
                if os.path.isfile(FullFilenameIn):
                        logging.info('Distributor: Input file Ok, exist in local folder')
                else:
                        logging.info('Distributor: Input file not exist in local folder')
                        resp = make_response('Input file not exist in local folder, maybe not copied from Web Resource \n')
                        resp.headers['WARNING'] = 'Input file not exist in local folder'
                        resp.status_code = 400
                        return resp
                # Build a AST Path to SCP
                # Consulta a que AST enviarlo (validacion a cuales enviar)
                HostsOut = []
                HostsOut = AskHostServ(CsIn,VdnIn)
                # Enviar los audios
                #print json.dumps({'HostsOut': HostsOut})
                FinalResponse = []
                ResponseOut = []
                if HostsOut:
                        logging.info('Distributor: HostOut not emtpy %s',json.dumps({'HostsOut': HostsOut}) )
                        for hostdest in HostsOut:
                                #print hostdest["IP"],hostdest["HOSTNAME"]
                                logging.info('Distributor: Host %s FileIn %s FileOut %s',hostdest,FullFilenameIn,FullFilenameOut)
                                # Try to Copy File
                                try:
                                        ResponseOut = CopyFile(FullFilenameIn,FullFilenameOut,hostdest["IP"],hostdest["HOSTNAME"])
                                        logging.info('ResponseOut %s',ResponseOut)
                                        FinalResponse.append(ResponseOut)
                                        logging.info('FinalResponse : %s',FinalResponse)
                                except OSError as e:
                                        logging.info('Distributor: ERROR Copy Failed OS EXCEPTION %s',str(e))
                                        #print(str(e)+" OS EXCEPTION")
                                        pass
                                except IOError as e:
                                        logging.info('Distributor: ERROR Copy Failed IOERROR',str(e))
                                        #print(str(e)+" IOERROR")
                                        pass
                                except Exception as e:
                                        logging.info('Distributor: ERROR Copy Failed OTHER EXCEPTION',str(e))
                                        #print(str(e)+" OTHER EXCEPTION")
                                        pass
                else:
                        logging.info('Distributor: HostOut is empty, will respond')
                        #print "HostOut is empty"
                        resp = make_response('No Host configured to distribute Audio. Please Check configuration \n')
                        resp.headers['WARNING'] = 'No Host configured to distribute audio. Please Check configuration for CS and VDN'
                        resp.status_code = 400
                        return resp

                # Generar y Enviar la respuesta
                resp = make_response(json.dumps(FinalResponse))
                resp.status_code = 200
                return resp

class GetInfo(Resource):
        def get(self):
                parser = reqparse.RequestParser()
                parser.add_argument("CS")
                parser.add_argument("VDN")
                args = parser.parse_args()
                try:
                        if args["CS"] and args["VDN"]:
                                CsIn = args["CS"].encode('utf8')
                                VdnIn = args["VDN"].encode('utf8')
                                logging.info('GetInfo: input data variables CS: %s VDN: %s',CsIn,VdnIn)
                        else:
                                resp = make_response("Insuficient data argument to process \n")
                                resp.headers['WARNING'] = 'Insuficient data argument to process'
                                resp.status_code = 418
                                return resp
                except (ValueError, KeyError, TypeError):
                        logging.info('GetInfo: Data Insuficient to process exception')
                        return "Data Insuficient to process exception", 418
                resp = []
                resp = AskDbServ(CsIn,VdnIn)
                return resp

class GetCSInfo(Resource):
        def get(self):
                parser = reqparse.RequestParser()
                parser.add_argument("CS")
                args = parser.parse_args()
                try:
                        if args["CS"]:
                                CsIn = args["CS"].encode('utf8')
                                logging.info('GetInfo: input data variables CS: %s ',CsIn)
                        else:
                                resp = make_response("Insuficient data argument to process \n")
                                resp.headers['WARNING'] = 'Insuficient data argument to process'
                                resp.status_code = 418
                                return resp
                except (ValueError, KeyError, TypeError):
                        logging.info('GetInfo: Data Insuficient to process exception')
                        return "Data Insuficient to process exception", 418
                resp = []
                resp = AskDbCS(CsIn)
                # print json.dumps({'resp':resp})
                return resp


###################### Main Loop ######################
logging.basicConfig(filename='../log/ServiceAudio.log',format='%(asctime)s %(message)s', datefmt='%Y%m%d %H:%M:%S ', level=logging.INFO)
logging.debug('Main: **** Starting Appliaton Distributor (Debug Level)')
logging.info('Main: **** Starting Appliaton Distributor (Info Level)')
logging.warning('Main: **** Starting Appliaton Distributor (Warning Level)')

api.add_resource(Distributor, "/Distributor/")
api.add_resource(GetInfo, "/GetInfo/")
api.add_resource(GetCSInfo, "/GetCSInfo/")
ServiceAudio.run(host='192.168.X.X',port=5000,debug=True)
