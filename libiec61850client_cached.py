#!/usr/bin/env python3

import os,sys
import ctypes
import time
import lib61850 # Pustaka yang benar untuk fungsi-fungsi terkait
import logging
import json

from urllib.parse import urlparse
from enum import Enum

# --- Nama file untuk menyimpan cache ---
CACHE_FILE = "ied_model_cache.json"

class AddCause(Enum):
        ADD_CAUSE_UNKNOWN = 0
        ADD_CAUSE_NOT_SUPPORTED = 1
        ADD_CAUSE_BLOCKED_BY_SWITCHING_HIERARCHY = 2
        ADD_CAUSE_SELECT_FAILED = 3
        ADD_CAUSE_INVALID_POSITION = 4
        ADD_CAUSE_POSITION_REACHED = 5
        ADD_CAUSE_PARAMETER_CHANGE_IN_EXECUTION = 6
        ADD_CAUSE_STEP_LIMIT = 7
        ADD_CAUSE_BLOCKED_BY_MODE = 8
        ADD_CAUSE_BLOCKED_BY_PROCESS = 9
        ADD_CAUSE_BLOCKED_BY_INTERLOCKING = 10
        ADD_CAUSE_BLOCKED_BY_SYNCHROCHECK = 11
        ADD_CAUSE_COMMAND_ALREADY_IN_EXECUTION = 12
        ADD_CAUSE_BLOCKED_BY_HEALTH = 13
        ADD_CAUSE_1_OF_N_CONTROL = 14
        ADD_CAUSE_ABORTION_BY_CANCEL = 15
        ADD_CAUSE_TIME_LIMIT_OVER = 16
        ADD_CAUSE_ABORTION_BY_TRIP = 17
        ADD_CAUSE_OBJECT_NOT_SELECTED = 18
        ADD_CAUSE_OBJECT_ALREADY_SELECTED = 19
        ADD_CAUSE_NO_ACCESS_AUTHORITY = 20
        ADD_CAUSE_ENDED_WITH_OVERSHOOT = 21
        ADD_CAUSE_ABORTION_DUE_TO_DEVIATION = 22
        ADD_CAUSE_ABORTION_BY_COMMUNICATION_LOSS = 23
        ADD_CAUSE_ABORTION_BY_COMMAND = 24
        ADD_CAUSE_NONE = 25
        ADD_CAUSE_INCONSISTENT_PARAMETERS = 26
        ADD_CAUSE_LOCKED_BY_OTHER_CLIENT = 27

class IedClientError(Enum):
    IED_ERROR_OK = 0
    IED_ERROR_NOT_CONNECTED = 1
    IED_ERROR_ALREADY_CONNECTED = 2
    IED_ERROR_CONNECTION_LOST = 3
    IED_ERROR_SERVICE_NOT_SUPPORTED = 4
    IED_ERROR_CONNECTION_REJECTED = 5
    IED_ERROR_OUTSTANDING_CALL_LIMIT_REACHED = 6
    IED_ERROR_USER_PROVIDED_INVALID_ARGUMENT = 10
    IED_ERROR_ENABLE_REPORT_FAILED_DATASET_MISMATCH = 11
    IED_ERROR_OBJECT_REFERENCE_INVALID = 12
    IED_ERROR_UNEXPECTED_VALUE_RECEIVED = 13
    IED_ERROR_TIMEOUT = 20
    IED_ERROR_ACCESS_DENIED = 21
    IED_ERROR_OBJECT_DOES_NOT_EXIST = 22
    IED_ERROR_OBJECT_EXISTS = 23
    IED_ERROR_OBJECT_ACCESS_UNSUPPORTED = 24
    IED_ERROR_TYPE_INCONSISTENT = 25
    IED_ERROR_TEMPORARILY_UNAVAILABLE = 26
    IED_ERROR_OBJECT_UNDEFINED = 27
    IED_ERROR_INVALID_ADDRESS = 28
    IED_ERROR_HARDWARE_FAULT = 29
    IED_ERROR_TYPE_UNSUPPORTED = 30
    IED_ERROR_OBJECT_ATTRIBUTE_INCONSISTENT = 31
    IED_ERROR_OBJECT_VALUE_INVALID = 32
    IED_ERROR_OBJECT_INVALIDATED = 33
    IED_ERROR_MALFORMED_MESSAGE = 34
    IED_ERROR_SERVICE_NOT_IMPLEMENTED = 98
    IED_ERROR_UNKNOWN = 99


logger = logging.getLogger(__name__)

class iec61850client():

        def __init__(self, readvaluecallback = None, loggerRef = None, cmdTerm_cb = None, Rpt_cb = None):
                global logger
                if loggerRef != None:
                        logger = loggerRef

                self.polling = {}
                self.connections = {}
                self.readvaluecallback = readvaluecallback
                self.cmdTerm_cb = cmdTerm_cb
                self.Rpt_cb = Rpt_cb
                self.cb_refs = []
                self.reporting = {}
                self.model_cache = self._load_cache()

        @staticmethod
        def _load_cache():
            if os.path.exists(CACHE_FILE):
                try:
                    with open(CACHE_FILE, 'r') as f:
                        logger.info(f"Memuat model IED dari cache: {CACHE_FILE}")
                        return json.load(f)
                except (IOError, json.JSONDecodeError) as e:
                    logger.warning(f"Gagal memuat cache, akan melakukan discovery ulang. Error: {e}")
                    return {}
            else:
                logger.info("File cache tidak ditemukan, akan dibuat setelah discovery.")
                return {}

        def _save_cache(self):
            try:
                with open(CACHE_FILE, 'w') as f:
                    logger.info(f"Menyimpan model IED ke cache: {CACHE_FILE}")
                    json.dump(self.model_cache, f, indent=4)
            except IOError as e:
                logger.error(f"Gagal menyimpan cache. Error: {e}")

        @staticmethod
        def printValue(value):
                _type = lib61850.MmsValue_getTypeString(value)
                _type = str(_type)
                if _type == "boolean":
                        return lib61850.MmsValue_getBoolean(value), _type
                if _type == "array":
                        return "arr", _type
                if _type == "bcd":
                        return "bcd", _type
                if _type == "binary-time":
                        return lib61850.MmsValue_getBinaryTimeAsUtcMs(value), _type
                if _type == "bit-string":
                        return lib61850.MmsValue_getBitStringAsInteger(value), _type
                if _type == "access-error":
                        return "ACCESS ERROR", _type
                if _type == "float":
                        return lib61850.MmsValue_toFloat(value), _type
                if _type == "generalized-time":
                        return lib61850.MmsValue_toUnixTimestamp(value), _type
                if _type == "integer":
                        return lib61850.MmsValue_toInt64(value), _type
                if _type == "oid":
                        return "OID ERROR", _type
                if _type == "mms-string":
                        return lib61850.MmsValue_toString(value).decode("utf-8"), _type
                if _type == "structure":
                        sub_list = []
                        size = lib61850.MmsValue_getArraySize(value)
                        for i in range(size):
                                component = lib61850.MmsValue_getElement(value, i)
                                if component:
                                        component_value, _ = iec61850client.printValue(component)
                                        sub_list.append(component_value)
                        return sub_list, _type
                if _type == "octet-string":
                        len_val = lib61850.MmsValue_getOctetStringSize(value)
                        buf = lib61850.MmsValue_getOctetStringBuffer(value)
                        buff = ctypes.cast(buf, ctypes.POINTER(ctypes.c_char))
                        res = bytearray(len_val)
                        rptr = (ctypes.c_char * len_val).from_buffer(res)
                        ctypes.memmove(rptr, buff, len_val)
                        return (''.join(format(x, '02x') for x in res)), _type
                if _type == "unsigned":
                        return lib61850.MmsValue_toUint32(value), _type
                if _type == "utc-time":
                        return lib61850.MmsValue_getUtcTimeInMs(value), _type
                if _type == "visible-string":
                        return lib61850.MmsValue_toString(value).decode("utf-8"), _type
                if _type == "unknown(error)":
                        return "UNKNOWN ERROR", _type
                return "CANNOT FIND TYPE", _type


        @staticmethod
        def printDataDirectory(con, doRef):
                tmodel = {}
                if doRef.find("/") == -1:
                        logger.error("invalid datadirecory")
                        return {}

                error = lib61850.IedClientError()
                dataAttributes = lib61850.IedConnection_getDataDirectoryFC(con, ctypes.byref(error), doRef.encode('utf-8'))

                if error.value != 0:
                        logger.error("could not get logical device list, error:%i" % error.value)

                if dataAttributes:
                        dataAttribute = lib61850.LinkedList_getNext(dataAttributes)

                        while dataAttribute:
                                daName = ctypes.cast(lib61850.LinkedList_getData(dataAttribute),ctypes.c_char_p).value.decode("utf-8")
                                daRef = doRef+"."+daName[:-4]
                                fcName = daName[-3:-1]

                                submodel = iec61850client.printDataDirectory(con,daRef)
                                if submodel:
                                        tmodel[daName[:-4]] = submodel

                                else:
                                        tmodel[daName[:-4]] = {}
                                        tmodel[daName[:-4]]['reftype'] = "DA"
                                        tmodel[daName[:-4]]['FC'] = fcName
                                        tmodel[daName[:-4]]['value'] = "UNKNOWN"
                                        fc = lib61850.FunctionalConstraint_fromString(fcName)
                                        value = lib61850.IedConnection_readObject(con, ctypes.byref(error), daRef.encode('utf-8'), fc)

                                        if error.value == 0:
                                                tmodel[daName[:-4]]['value'], tmodel[daName[:-4]]['type'] = iec61850client.printValue(value)
                                                lib61850.MmsValue_delete(value)

                                dataAttribute = lib61850.LinkedList_getNext(dataAttribute)

                        lib61850.LinkedList_destroy(dataAttributes)
                return tmodel


        @staticmethod
        def discovery(con):
                tmodel = {}

                error = lib61850.IedClientError()
                deviceList = lib61850.IedConnection_getLogicalDeviceList(con, ctypes.byref(error))

                if error.value != 0:
                        logger.error("could not get logical device list, error:%i" % error.value)
                        return {}

                if deviceList:
                        device = lib61850.LinkedList_getNext(deviceList)
                        while device:
                                LD_name=ctypes.cast(lib61850.LinkedList_getData(device),ctypes.c_char_p).value.decode("utf-8")
                                tmodel[LD_name] = {}

                                logicalNodes = lib61850.IedConnection_getLogicalDeviceDirectory(con, ctypes.byref(error), LD_name.encode('utf-8'))
                                if error.value != 0:
                                        lib61850.LinkedList_destroy(deviceList)
                                        return {}

                                logicalNode = lib61850.LinkedList_getNext(logicalNodes)
                                while logicalNode:
                                        LN_name=ctypes.cast(lib61850.LinkedList_getData(logicalNode),ctypes.c_char_p).value.decode("utf-8")
                                        tmodel[LD_name][LN_name] = {}

                                        LNobjects = lib61850.IedConnection_getLogicalNodeDirectory(con, ctypes.byref(error), (LD_name+"/"+LN_name).encode('utf-8'),lib61850.ACSI_CLASS_DATA_OBJECT)
                                        if error.value != 0:
                                                lib61850.LinkedList_destroy(logicalNodes)
                                                lib61850.LinkedList_destroy(deviceList)
                                                return {}

                                        LNobject = lib61850.LinkedList_getNext(LNobjects)
                                        while LNobject:
                                                Do = ctypes.cast(lib61850.LinkedList_getData(LNobject),ctypes.c_char_p).value.decode("utf-8")
                                                tmodel[LD_name][LN_name][Do] = {}
                                                doRef = LD_name+"/"+LN_name+"."+Do
                                                tmodel[LD_name][LN_name][Do] = iec61850client.printDataDirectory(con, doRef)
                                                LNobject = lib61850.LinkedList_getNext(LNobject)
                                        lib61850.LinkedList_destroy(LNobjects)

                                        LNdss = lib61850.IedConnection_getLogicalNodeDirectory(con, ctypes.byref(error), (LD_name+"/"+LN_name).encode('utf-8'), lib61850.ACSI_CLASS_DATA_SET)
                                        if error.value != 0:
                                                lib61850.LinkedList_destroy(logicalNodes)
                                                lib61850.LinkedList_destroy(deviceList)
                                                return tmodel

                                        LNds = lib61850.LinkedList_getNext(LNdss)
                                        while LNds:
                                                DSname = ctypes.cast(lib61850.LinkedList_getData(LNds),ctypes.c_char_p).value.decode("utf-8")
                                                tmodel[LD_name][LN_name][DSname] = {}
                                                isDel = ctypes.c_bool(False)
                                                dataSetMembers = lib61850.IedConnection_getDataSetDirectory(con, ctypes.byref(error), (LD_name+"/"+LN_name+"."+DSname).encode('utf-8'), ctypes.byref(isDel))
                                                if error.value != 0:
                                                        lib61850.LinkedList_destroy(LNdss)
                                                        lib61850.LinkedList_destroy(logicalNodes)
                                                        lib61850.LinkedList_destroy(deviceList)
                                                        return tmodel

                                                if isDel.value:
                                                        logger.info("  DS: %s, is Deletable" % DSname)
                                                else:
                                                        logger.info("  DS: %s, not Deletable" % DSname)
                                                dataSetMemberRef = lib61850.LinkedList_getNext(dataSetMembers)
                                                i = 0
                                                while dataSetMemberRef:
                                                        dsRef = ctypes.cast(lib61850.LinkedList_getData(dataSetMemberRef),ctypes.c_char_p).value.decode("utf-8")
                                                        DX = dsRef[:-4]
                                                        FC = dsRef[-3:-1]
                                                        tmodel[LD_name][LN_name][DSname][str(i)] = {}
                                                        tmodel[LD_name][LN_name][DSname][str(i)]['reftype'] = "DX"
                                                        tmodel[LD_name][LN_name][DSname][str(i)]['type'] = "reference"
                                                        tmodel[LD_name][LN_name][DSname][str(i)]['value'] = DX
                                                        tmodel[LD_name][LN_name][DSname][str(i)]['FC'] = FC
                                                        dataSetMemberRef = lib61850.LinkedList_getNext(dataSetMemberRef)
                                                        i += 1
                                                lib61850.LinkedList_destroy(dataSetMembers)
                                                LNds = lib61850.LinkedList_getNext(LNds)
                                        lib61850.LinkedList_destroy(LNdss)

                                        LNrpp = lib61850.IedConnection_getLogicalNodeDirectory(con, ctypes.byref(error), (LD_name+"/"+LN_name).encode('utf-8'), lib61850.ACSI_CLASS_URCB)
                                        if error.value != 0:
                                                lib61850.LinkedList_destroy(logicalNodes)
                                                lib61850.LinkedList_destroy(deviceList)
                                                return tmodel

                                        LNrp = lib61850.LinkedList_getNext(LNrpp)
                                        while LNrp:
                                                Rp = ctypes.cast(lib61850.LinkedList_getData(LNrp),ctypes.c_char_p).value.decode("utf-8")
                                                tmodel[LD_name][LN_name][Rp] = {}
                                                doRef = LD_name+"/"+LN_name+"."+Rp
                                                tmodel[LD_name][LN_name][Rp] = iec61850client.printDataDirectory(con, doRef)
                                                LNrp = lib61850.LinkedList_getNext(LNrp)
                                        lib61850.LinkedList_destroy(LNrpp)

                                        LNbrr = lib61850.IedConnection_getLogicalNodeDirectory(con, ctypes.byref(error), (LD_name+"/"+LN_name).encode('utf-8'), lib61850.ACSI_CLASS_BRCB)
                                        if error.value != 0:
                                                lib61850.LinkedList_destroy(logicalNodes)
                                                lib61850.LinkedList_destroy(deviceList)
                                                return tmodel

                                        LNbr = lib61850.LinkedList_getNext(LNbrr)
                                        while LNbr:
                                                Br = ctypes.cast(lib61850.LinkedList_getData(LNbr),ctypes.c_char_p).value.decode("utf-8")
                                                tmodel[LD_name][LN_name][Br] = {}
                                                doRef = LD_name+"/"+LN_name+"."+Br
                                                tmodel[LD_name][LN_name][Br] = iec61850client.printDataDirectory(con, doRef)
                                                LNbr = lib61850.LinkedList_getNext(LNbr)
                                        lib61850.LinkedList_destroy(LNbrr)

                                        logicalNode = lib61850.LinkedList_getNext(logicalNode)
                                lib61850.LinkedList_destroy(logicalNodes)
                                device = lib61850.LinkedList_getNext(device)
                        lib61850.LinkedList_destroy(deviceList)
                return tmodel

        @staticmethod
        def getMMsValue(typeVal, value, size=8, typeval = -1):
                if typeVal == "visible-string" or typeval == lib61850.MMS_VISIBLE_STRING:
                        return lib61850.MmsValue_newVisibleString(str(value))
                if typeVal == "boolean" or typeval == lib61850.MMS_BOOLEAN:
                        if (type(value) is str and value.lower() == "true") or (type(value) is bool and value == True):
                                return lib61850.MmsValue_newBoolean(True)
                        else:
                                return lib61850.MmsValue_newBoolean(False)
                if typeVal == "integer" or typeval == lib61850.MMS_INTEGER:
                        return lib61850.MmsValue_newInteger(int(value))
                if typeVal == "unsigned" or typeval == lib61850.MMS_UNSIGNED:
                        return lib61850.MmsValue_newUnsignedFromUint32(int(value))
                if typeVal == "mms-string" or typeval == lib61850.MMS_STRING:
                        return lib61850.MmsValue_newMmsString(str(value))
                if typeVal == "float" or typeval == lib61850.MMS_FLOAT:
                        return lib61850.MmsValue_newFloat(float(value))
                if typeVal == "binary-time" or typeval == lib61850.MMS_BINARY_TIME:
                        return lib61850.MmsValue_newBinaryTime(int(value))
                if typeVal == "bit-string" or typeval == lib61850.MMS_BIT_STRING:
                        bs = lib61850.MmsValue_newBitString(size)
                        return lib61850.MmsValue_setBitStringFromInteger(bs,int(value))
                if typeVal == "generalized-time" or typeval == lib61850.MMS_GENERALIZED_TIME:
                        return lib61850.MmsValue_newUtcTimeByMsTime(int(value))
                if typeVal == "utc-time" or typeval == lib61850.MMS_UTC_TIME:
                        return lib61850.MmsValue_newUtcTimeByMsTime(int(value))
                if typeVal == "octet-string" or typeval == lib61850.MMS_OCTET_STRING:
                        sl = len(value)
                        sptr = (ctypes.c_char * sl).from_buffer(value)
                        buf = lib61850.MmsValue_newOctetString(sl,127)
                        buff = ctypes.cast(int(buf), ctypes.POINTER(ctypes.c_char))
                        ctypes.memmove(buff, sptr, sl)
                        return buf
                if typeVal in ("array", "bcd", "access-error", "oid", "structure", "unknown(error)") or \
                   typeval in (lib61850.MMS_ARRAY, lib61850.MMS_BCD, lib61850.MMS_DATA_ACCESS_ERROR, lib61850.MMS_OBJ_ID, lib61850.MMS_STRUCTURE):
                        return None
                logger.error("Mms value type %s not supported" % typeVal)
                return None

        @staticmethod
        def writeValue(con, model, ref, value):
                submodel, path = iec61850client.parseRef(model,ref)

                if not submodel:
                        logger.error("cannot find ref: %s in model" % ref)
                        return {},-1
                if 'FC' not in submodel:
                        logger.error("ref is not DA")
                        return {},-1

                fc = lib61850.FunctionalConstraint_fromString(submodel['FC'])
                mmsvalue = iec61850client.getMMsValue(submodel['type'],value)
                if not mmsvalue:
                        return model,-1

                error = lib61850.IedClientError()
                lib61850.IedConnection_writeObject(con, ctypes.byref(error), ref.encode('utf-8'), fc, mmsvalue)
                lib61850.MmsValue_delete(mmsvalue)
                if error.value == 0:
                        model, err = iec61850client.updateValueInModel(con, model, ref)
                        return model, err
                return model, error.value

        @staticmethod
        def updateValueInModel(con, model, ref):
                err = -1
                val, path = iec61850client.parseRef(model,ref)

                def update_recurse(con, submodel, path):
                        err = -1
                        if len(path) < 1:
                                logger.error("recursion into model went wrong")
                                err = -1
                        elif len(path) == 1:
                                if submodel[ path[0] ] and 'reftype' in submodel[ path[0] ] and submodel[ path[0] ]['reftype'] == 'DA':
                                        fcName = submodel[ path[0] ]['FC']
                                        fc = lib61850.FunctionalConstraint_fromString(fcName)
                                        error = lib61850.IedClientError()
                                        value = lib61850.IedConnection_readObject(con, ctypes.byref(error), ref.encode('utf-8'), fc)
                                        if error.value == 0:
                                                submodel[ path[0] ]['value'], submodel[ path[0] ]['type'] = iec61850client.printValue(value)
                                                lib61850.MmsValue_delete(value)
                                                err = 0
                                        else:
                                                logger.error("could not read DA: %s from device" % ref)
                                                err = error.value

                                else:
                                        submodel[ path[0] ] = iec61850client.printDataDirectory(con, ref)
                                        if submodel[ path[0] ]:
                                                err = 0
                                        else:
                                                err = -1
                        else:
                                submodel[ path[0] ], err = update_recurse(con, submodel[ path[0] ], path[1:])
                        return submodel, err

                model, err = update_recurse(con, model, path)
                return model, err


        @staticmethod
        def parseRef(model,ref):
                path = []
                if not ref:
                        return model, path

                _ref = ref.split("/")
                if len(_ref) == 1:
                        path.append(ref)
                        return (model.get(ref), path) if ref in model else ({}, [])

                if len(_ref) > 2:
                        logger.error("cannot parse ref, more than 1 '/' encountered ")
                        return {}, []

                LD = _ref[0]
                path.append(LD)

                mm = model.get(LD)
                if not mm:
                        logger.error("cannot find LD in model")
                        return {}, []

                _ref_parts = _ref[1].split(".")
                for part in _ref_parts:
                        path.append(part)
                        mm = mm.get(part)
                        if not mm:
                                logger.error("cannot find node in model: %s" % part)
                                return {},[]

                return mm, path


        @staticmethod
        def getRef(model,path):
                ref = ""
                mm = model
                for i, part in enumerate(path):
                        if part not in mm:
                                logger.error("cannot find node in model: %s in %s" % (part,ref))
                                return ref, mm
                        if i == 1:
                                ref += "/"
                        elif i > 1:
                                ref += "."
                        ref += part
                        mm = mm[part]
                return ref, mm


        @staticmethod
        def printrefs(model, ref="", depth=0):
                for element, content in model.items():
                        _ref = f"{ref}/{element}" if depth == 1 else f"{ref}.{element}" if depth > 1 else element
                        if isinstance(content, dict) and 'value' in content:
                                print(f"{_ref}:\t{content['value']}")
                        elif isinstance(content, dict):
                                iec61850client.printrefs(content, _ref, depth + 1)


        def getIED(self, host, port):
            port = port or 102
            if not host:
                logger.error("missing hostname")
                return -1

            tupl = f"{host}:{port}"
            if tupl in self.connections and self.connections[tupl].get("con"):
                if self.connections[tupl].get("model"):
                    return 0
                else:
                    logger.warning(f"Connection for {tupl} exists but model is missing. Rediscovering.")
                    con = self.connections[tupl]["con"]
                    model = iec61850client.discovery(con)
                    if model:
                        self.connections[tupl]["model"] = model
                        self.model_cache[tupl] = model
                        self._save_cache()
                        return 0
                    else:
                        lib61850.IedConnection_destroy(con)
                        self.connections[tupl]["con"] = None
                        return -1

            if tupl in self.model_cache:
                logger.info(f"Menggunakan model dari cache untuk IED: {tupl}")
                self.connections[tupl] = {"con": None, "model": self.model_cache[tupl]}
            else:
                self.connections[tupl] = {"con": None, "model": {}}

            # --- KODE YANG DIPERBAIKI ---
            con = lib61850.IedConnection_create()
            error = lib61850.IedClientError()
            lib61850.IedConnection_connect(con,ctypes.byref(error), host.encode('utf-8'), port)

            if error.value == lib61850.IED_ERROR_OK:
            # --- AKHIR DARI KODE YANG DIPERBAIKI ---
                self.connections[tupl]["con"] = con

                if not self.connections[tupl]["model"]:
                    logger.info(f"Melakukan discovery penuh untuk IED: {tupl}")
                    model = iec61850client.discovery(con)
                    if model:
                        self.connections[tupl]["model"] = model
                        self.model_cache[tupl] = model
                        self._save_cache()
                    else:
                        logger.error(f"Discovery gagal untuk IED: {tupl}")
                        lib61850.IedConnection_destroy(con)
                        self.connections[tupl]["con"] = None
                        return -1

                if tupl in self.reporting:
                    for refdata in self.reporting[tupl]:
                        rcb = refdata["rcb"]
                        err_rcb = lib61850.IedClientError()
                        rcb = lib61850.IedConnection_getRCBValues(con, ctypes.byref(err_rcb), refdata["RPT"].encode('utf-8'), rcb)
                        RptId = lib61850.ClientReportControlBlock_getRptId(rcb)
                        lib61850.IedConnection_installReportHandler(con, refdata["RPT"].encode('utf-8'), RptId.encode('utf-8'), refdata["cbh"], id(refdata["refdata"]))
                        lib61850.ClientReportControlBlock_setRptEna(rcb, True)
                        lib61850.ClientReportControlBlock_setGI(rcb, True)
                        lib61850.IedConnection_setRCBValues(con, ctypes.byref(err_rcb), rcb, lib61850.RCB_ELEMENT_RPT_ENA | lib61850.RCB_ELEMENT_GI, False)
                        refdata["rcb"] = rcb
                return 0
            else:
                logger.error(f"Koneksi ke IED gagal: {host}:{port}, error: {error.value}")
                lib61850.IedConnection_destroy(con) # Perbaikan: Gunakan lib61850
                return -1

        def registerWriteValue(self, ref, value):
                uri_ref = urlparse(ref)
                port = uri_ref.port or 102
                if uri_ref.scheme != "iec61850":
                        logger.error(f"incorrect scheme, only iec61850 is supported, not {uri_ref.scheme}")
                        return -1, "incorrect scheme"
                if not uri_ref.hostname:
                        logger.error(f"missing hostname: {ref}")
                        return -1, "missing hostname"

                tupl = f"{uri_ref.hostname}:{port}"
                if self.getIED(uri_ref.hostname, port) == 0:
                        con = self.connections[tupl]['con']
                        model = self.connections[tupl]['model']
                        if not con or not model:
                                logger.error("no valid connection or model")
                                return -1, "no valid connection or model"

                        _, error = iec61850client.writeValue(con, model, uri_ref.path[1:], value)
                        if error == 0:
                                submodel, _ = iec61850client.parseRef(model,uri_ref.path[1:])
                                logger.debug(f"Value '{submodel}' written to {ref}")
                                if self.readvaluecallback:
                                        self.readvaluecallback(ref, submodel)
                                return 0, "no error"
                        else:
                                logger.error(f"could not write '{value}' to {ref} with error: {error}")
                                if error == 3:
                                        lib61850.IedConnection_destroy(con)
                                        self.connections[tupl]['con'] = None
                                return error, "write failed"
                else:
                        logger.error(f"no connection to IED: {uri_ref.hostname}:{port}")
                return -1, "no connection"


        def ReadValue(self, ref):
                uri_ref = urlparse(ref)
                port = uri_ref.port or 102
                if uri_ref.scheme != "iec61850":
                        logger.error(f"incorrect scheme, only iec61850 is supported, not {uri_ref.scheme}")
                        return {}, -1
                if not uri_ref.hostname:
                        logger.error(f"missing hostname: {ref}")
                        return {}, -1

                tupl = f"{uri_ref.hostname}:{port}"
                if self.getIED(uri_ref.hostname, port) == 0:
                        con = self.connections[tupl]['con']
                        model = self.connections[tupl]['model']
                        if not con or not model:
                                logger.error("no valid connection or model")
                                return {}, -1

                        submodel_check, _ = iec61850client.parseRef(model, uri_ref.path[1:])
                        if submodel_check:
                                model, error = iec61850client.updateValueInModel(con, model, uri_ref.path[1:])
                                if error == 0:
                                        self.connections[tupl]['model'] = model
                                        submodel, _ = iec61850client.parseRef(model, uri_ref.path[1:])
                                        logger.debug(f"Value '{submodel}' read from {ref}")
                                        if self.readvaluecallback:
                                                self.readvaluecallback(ref, submodel)
                                        return submodel, 0
                                else:
                                        logger.error(f"could not read '{ref}' with error: {error}")
                                        if error == 3:
                                                lib61850.IedConnection_destroy(con)
                                                self.connections[tupl]['con'] = None
                        else:
                                logger.error(f"could not find {uri_ref.path[1:]} in model")
                else:
                        logger.error(f"no connection to IED: {uri_ref.hostname}:{port}")
                return {}, -1

        def ReportHandler_cb(self, param, report):
                refdata = ctypes.cast(param, ctypes.py_object).value
                key, tupl, LD, LN, DSRef = refdata

                dataSetValues = lib61850.ClientReport_getDataSetValues(report)
                if not dataSetValues:
                        return

                dataset = self.connections[tupl]['model'][LD][LN][DSRef]
                for index_str in dataset:
                        index = int(index_str)
                        reason = lib61850.ClientReport_getReasonForInclusion(report, index)
                        if reason != lib61850.IEC61850_REASON_NOT_INCLUDED:
                                mmsval = lib61850.MmsValue_getElement(dataSetValues, index)
                                if mmsval:
                                        DaRef = dataset[index_str]['value']
                                        val, _type = iec61850client.printValue(mmsval)
                                        logger.debug(f"{DaRef}: {val} ({_type})")

                                        submodel, _ = iec61850client.parseRef(self.connections[tupl]['model'], DaRef)
                                        if submodel:
                                                submodel["value"] = val
                                                if self.Rpt_cb:
                                                        self.Rpt_cb(DaRef, submodel)


        def registerForReporting(self, key, tupl, ref_path):
                con = self.connections[tupl]['con']
                model = self.connections[tupl]['model']
                found = False

                for LD_name, lns in model.items():
                        if found: break
                        for LN_name, dos in lns.items():
                                if found: break
                                for DSname, ds_content in dos.items():
                                        if isinstance(ds_content, dict) and ds_content.get("0", {}).get('reftype') == "DX":
                                                for index, dx_info in ds_content.items():
                                                        if ref_path.startswith(dx_info['value']):
                                                                logger.info(f"DATASET found! Ref:{ref_path} in DSref: {dx_info['value']}")
                                                                DSRef = f"{LD_name}/{LN_name}.{DSname}"
                                                                logger.info(f"  DSRef:{DSRef}")

                                                                for RP_name, rp_content in dos.items():
                                                                        if isinstance(rp_content, dict) and rp_content.get("DatSet", {}).get("value") == f"{LD_name}/{LN_name}${DSname}":
                                                                                logger.info(f"RPT found! Ref:{LD_name}/{LN_name}.{RP_name}")
                                                                                RPT_path = f"{LD_name}/{LN_name}.{rp_content['DatSet']['FC']}.{RP_name}"

                                                                                error = lib61850.IedClientError()
                                                                                if RPT_path in self.cb_refs:
                                                                                        logger.info("RPT already registered")
                                                                                        return True

                                                                                rcb = lib61850.IedConnection_getRCBValues(con, ctypes.byref(error), RPT_path.encode('utf-8'), None)
                                                                                RptId = lib61850.ClientReportControlBlock_getRptId(rcb)

                                                                                cbh = lib61850.ReportCallbackFunction(self.ReportHandler_cb)
                                                                                refdata = [key, tupl, LD_name, LN_name, DSname]
                                                                                param_id = id(refdata)
                                                                                lib61850.IedConnection_installReportHandler(con, RPT_path.encode('utf-8'), RptId, cbh, param_id)

                                                                                if not lib61850.ClientReportControlBlock_getRptEna(rcb):
                                                                                        lib61850.ClientReportControlBlock_setRptEna(rcb, True)
                                                                                        lib61850.ClientReportControlBlock_setGI(rcb, True)
                                                                                        lib61850.IedConnection_setRCBValues(con, ctypes.byref(error), rcb, lib61850.RCB_ELEMENT_RPT_ENA | lib61850.RCB_ELEMENT_GI, True)

                                                                                self.cb_refs.append(RPT_path)
                                                                                if tupl not in self.reporting: self.reporting[tupl] = []
                                                                                self.reporting[tupl].append({"rcb": rcb, "cbh": cbh, "RPT": RPT_path, "refdata": refdata})

                                                                                logger.info("RPT registered successfully")
                                                                                return True
                                                                logger.error("Could not find report for dataset")
                                                                return False

                logger.error(f"RPT: could not find dataset for ref: {ref_path}")
                return False


        def registerReadValue(self,ref):
                uri_ref = urlparse(ref)
                port = uri_ref.port or 102
                if uri_ref.scheme != "iec61850":
                        logger.error(f"incorrect scheme, only iec61850 is supported, not {uri_ref.scheme}")
                        return -1
                if not uri_ref.hostname:
                        logger.error(f"missing hostname: {ref}")
                        return -1

                tupl = f"{uri_ref.hostname}:{port}"
                if self.getIED(uri_ref.hostname, port) == 0:
                        model = self.connections[tupl]['model']
                        submodel, _ = iec61850client.parseRef(model, uri_ref.path[1:])
                        if submodel:
                                if not self.registerForReporting(ref, tupl, uri_ref.path[1:]):
                                        self.polling[ref] = 1
                                return 0
                        else:
                                logger.error(f"could not find {uri_ref.path[1:]} in model")
                else:
                        logger.error(f"no connection to IED: {uri_ref.hostname}:{port}, ref:{ref} not registered")
                return -1


        def poll(self):
                for key, val in list(self.polling.items()):
                        uri_ref = urlparse(key)
                        port = uri_ref.port or 102
                        if uri_ref.scheme != "iec61850" or not uri_ref.hostname:
                                continue

                        tupl = f"{uri_ref.hostname}:{port}"
                        if self.getIED(uri_ref.hostname, port) == 0:
                                con = self.connections[tupl]['con']
                                model = self.connections[tupl]['model']
                                if con and model:
                                        _, err = iec61850client.updateValueInModel(con, model, uri_ref.path[1:])
                                        if err == 0:
                                                submodel, _ = iec61850client.parseRef(model, uri_ref.path[1:])
                                                logger.debug(f"value:{submodel} read from key: {key}")
                                                if self.readvaluecallback:
                                                        self.readvaluecallback(key, submodel)
                                        elif err == 3:
                                                lib61850.IedConnection_destroy(con)
                                                self.connections[tupl]['con'] = None


        def getDatamodel(self, ref=None, hostname="localhost", port=102):
                if ref:
                        uri_ref = urlparse(ref)
                        hostname = uri_ref.hostname
                        port = uri_ref.port

                port = port or 102
                if self.getIED(hostname, port) == 0:
                        tupl = f"{hostname}:{port}"
                        return self.connections[tupl]['model']
                else:
                        logger.debug(f"no connection to IED: {hostname}:{port}")
                        return {}


        def getRegisteredIEDs(self):
                return self.connections


        def commandTerminationHandler_cb(self, param, con):
                buff = ctypes.cast(param,ctypes.c_char_p).value.decode("utf-8")
                lastApplError = lib61850.ControlObjectClient_getLastApplError(con)
                if self.cmdTerm_cb:
                        if lastApplError.error != 0:
                                addCause = AddCause(lastApplError.addCause).name
                                self.cmdTerm_cb(f"object:{buff} Received CommandTermination-, LastApplError: {lastApplError.error}, addCause: {addCause}")
                        else:
                                self.cmdTerm_cb(f"object:{buff} Received CommandTermination+")


        def get_controlObject(self, tupl, uri_ref):
                con = self.connections[tupl]['con']
                self.connections[tupl].setdefault('control', {})

                ref_path = uri_ref.path[1:]
                if ref_path not in self.connections[tupl]['control']:
                        control = lib61850.ControlObjectClient_create(ref_path.encode('utf-8'), con)
                        self.connections[tupl]['control'][ref_path] = control

                        ctlModel = lib61850.ControlObjectClient_getControlModel(control)
                        if ctlModel in (lib61850.CONTROL_MODEL_DIRECT_ENHANCED, lib61850.CONTROL_MODEL_SBO_ENHANCED):
                                logger.info("control object: enhanced security")
                                cbh = lib61850.CommandTerminationHandler(self.commandTerminationHandler_cb)
                                ref_bytes = ref_path.encode('utf-8')
                                lib61850.ControlObjectClient_setCommandTerminationHandler(control, cbh, ctypes.c_char_p(ref_bytes))
                                self.cb_refs.extend([cbh, ref_bytes])
                        else:
                                logger.info("control object: normal security")
                return self.connections[tupl]['control'][ref_path]


        def operate(self, ref, value):
                uri_ref = urlparse(ref)
                hostname = uri_ref.hostname
                port = uri_ref.port or 102

                if self.getIED(hostname, port) == 0:
                        tupl = f"{hostname}:{port}"
                        control = self.get_controlObject(tupl, uri_ref)

                        lib61850.ControlObjectClient_setOrigin(control, b"mmi", 3)
                        mmsType = lib61850.ControlObjectClient_getCtlValType(control)
                        ctlVal = iec61850client.getMMsValue("", value, 0, mmsType)

                        error_code = lib61850.ControlObjectClient_operate(control, ctlVal, 0)
                        lib61850.MmsValue_delete(ctlVal)

                        if error_code == 1:
                                logger.info(f"operate: {value} returned successfully")
                                return 1, ""
                        else:
                                logger.error(f"operate: {value} returned failed")
                                lastApplError = lib61850.ControlObjectClient_getLastApplError(control)
                                addCause = AddCause(lastApplError.addCause).name
                                logger.info(f"LastApplError: {lastApplError.error}, addCause: {addCause}")
                                return error_code, addCause
                return -1, ""

        def select(self, ref, value):
                uri_ref = urlparse(ref)
                hostname = uri_ref.hostname
                port = uri_ref.port or 102
                addCause = ""
                error_code = -1

                if self.getIED(hostname, port) == 0:
                        tupl = f"{hostname}:{port}"
                        control = self.get_controlObject(tupl, uri_ref)
                        ctlModel = lib61850.ControlObjectClient_getControlModel(control)

                        if ctlModel == lib61850.CONTROL_MODEL_SBO_NORMAL:
                                logger.debug("SBO ctlmodel")
                                error_code = lib61850.ControlObjectClient_select(control)
                        elif ctlModel == lib61850.CONTROL_MODEL_SBO_ENHANCED:
                                logger.debug("SBOw ctlmodel")
                                mmsType = lib61850.ControlObjectClient_getCtlValType(control)
                                ctlVal = iec61850client.getMMsValue("", value, 0, mmsType)
                                lib61850.ControlObjectClient_setOrigin(control, b"mmi", 3)
                                error_code = lib61850.ControlObjectClient_selectWithValue(control, ctlVal)
                                lib61850.MmsValue_delete(ctlVal)
                        else:
                                addCause = f"cannot select object with ctlmodel: {ctlModel}"
                                logger.error(addCause)
                                return -1, addCause

                        if error_code == 1:
                                logger.info(f"select: {value} returned successfully")
                        else:
                                logger.error(f"select: {value} returned failed")
                                lastApplError = lib61850.ControlObjectClient_getLastApplError(control)
                                addCause = AddCause(lastApplError.addCause).name
                                logger.error(f"LastApplError: {lastApplError.error}, addCause: {addCause}")

                return error_code, addCause

        def cancel(self, ref):
                uri_ref = urlparse(ref)
                hostname = uri_ref.hostname
                port = uri_ref.port or 102
                if self.getIED(hostname, port) == 0:
                        tupl = f"{hostname}:{port}"
                        control = self.get_controlObject(tupl, uri_ref)
                        return lib61850.ControlObjectClient_cancel(control)
                return -1



if __name__=="__main__":
        logging.basicConfig(format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', level=logging.DEBUG)
        logger.debug("started")

        hostname = sys.argv[1] if len(sys.argv) > 1 else "localhost"
        tcpPort = int(sys.argv[2]) if len(sys.argv) > 2 else 102

        cl = iec61850client()
        model = cl.getDatamodel(hostname=hostname, port=tcpPort)
        cl.printrefs(model)

        # Example control operations
        for i in range(1, 3):
                ref = f"iec61850://{hostname}:{tcpPort}/IED1_XCBRGenericIO/CSWI{i}.Pos"
                error, cause = cl.select(ref, "True")
                if error == 1:
                        print(f"CSWI{i} selected successfully")
                        error, cause = cl.operate(ref, "True")
                        if error == 1:
                                print(f"CSWI{i} operated successfully")
                        else:
                                print(f"Failed to operate CSWI{i}")
                else:
                        print(f"Failed to select CSWI{i}")
