package ganyi.hadoop.replayer.rpc.param;


import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.xml.bind.DatatypeConverter;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by ganyi on 4/15/2017.
 */
class UtilJson {
    public static void main(String[] argv) {
        Long a = Long.valueOf("4294967240");
        int b = ((a & 0x80000000) == 0) ? a.intValue() : (-1) * Integer.valueOf(0x7fffffff - Long.valueOf(a & 0x7fffffff).intValue());
        System.out.printf("%d,%d\n", a, b);
    }

    public static String getString(JSONObject object, String src) {
        if (object.has(src)) {
            return object.getString(src);
        } else {
            return "";
        }
    }

    public static boolean getBoolean(JSONObject object, String bool) {
        return object.getBoolean(bool);
    }

    public static long getLong(JSONObject object, String value) {
        if (object.has(value)) {
            return object.getLong(value);
        } else {
            return 0;
        }
    }

    public static float getFloat(JSONObject object, String key) {
        if (object.has(key)) {
            return Float.valueOf(object.getString(key));
        } else {
            throw new RuntimeException("getFloat: cannot find value by key " + key);
        }
    }

    public static FsPermission getFsPermission(JSONObject object, String perm) {
        if (object.has(perm)) {
            JSONObject permission = object.getJSONObject(perm);
            if (permission.has("perm")) {
                short acl = (short) permission.getLong("perm");
                return new FsPermission(acl);
            } else {
                return FsPermission.getFileDefault();
            }
        } else {
            return FsPermission.getFileDefault();
        }
    }

    public static int getInt(JSONObject object, String key) {
        return object.getInt(key);
        /*Long a = Long.valueOf(String.valueOf(object.get(key)));
        return ((a & 0x80000000) == 0)?
                a.intValue():
                (-1)*Integer.valueOf(0x7fffffff - Long.valueOf(a&0x7fffffff).intValue());*/
    }

    public static short getShort(JSONObject object, String key) {
        return (short) getLong(object, key);
    }

    public static ExtendedBlock getExtendedBlock(JSONObject object, String block) {
        if (object.has(block)) {
            JSONObject extendBlockJson = object.getJSONObject(block);
            String poolID = extendBlockJson.getString("poolId");
            long blockID = extendBlockJson.getLong("blockId");
            long generationStamp = extendBlockJson.getLong("generationStamp");
            long numBytes = extendBlockJson.getLong("numBytes");
            return new ExtendedBlock(poolID, blockID, numBytes, generationStamp);
        } else {
            return null;
        }
    }

    public static EnumSetWritable<CreateFlag> getCreateFlag(JSONObject object, String createFlag) {
        if (object.has(createFlag)) {
            short flagValue = getShort(object, createFlag);
            EnumSet<CreateFlag> flags = EnumSet.noneOf(CreateFlag.class);
            if ((flagValue & 0x01) != 0) {
                flags.add(CreateFlag.CREATE);
            }
            if ((flagValue & 0x02) != 0) {
                flags.add(CreateFlag.OVERWRITE);
            }
            if ((flagValue & 0x04) != 0) {
                flags.add(CreateFlag.APPEND);
            }
            if ((flagValue & 0x08) != 0) {
                flags.add(CreateFlag.SYNC_BLOCK);
            }
            if ((flagValue & 0x10) != 0) {
                flags.add(CreateFlag.LAZY_PERSIST);
            }
            if ((flagValue & 0x20) != 0) {
                flags.add(CreateFlag.NEW_BLOCK);
            }
            return new EnumSetWritable<CreateFlag>(flags);
        } else {
            return null;
        }
    }

    public static CryptoProtocolVersion[] getcryptoProtocolVersion(JSONObject object, String key) {
        if (object.has(key)) {
            return CryptoProtocolVersion.supported();
        } else {
            return CryptoProtocolVersion.supported();
        }
    }

    public static DatanodeInfo[] getDatanodeInfo(JSONObject object, String key) {
        if (object.has(key)) {
            return null;
        } else {
            return null;
        }
    }

    public static DatanodeID getDatanodeID(JSONObject object, String key) {
        JSONObject DatanodeIDJson = object.getJSONObject(key);
        DatanodeIDParam param = DatanodeIDParam.parseJson(DatanodeIDJson);
        return new DatanodeID(param.getIpAddr(), param.getHostName(), param.getDatanodeUuid(),
                param.getXferPort(), param.getInfoPort(), param.getInfoSecurePort(), param.getIpcPort());
    }

    public static ExportedBlockKeys getExportedBlockKeys(JSONObject object, String key) {
        if (object.has(key)) {
            return new ExportedBlockKeys();
        } else {
            return new ExportedBlockKeys();
        }
    }

    public static StorageInfo getStorageInfo(JSONObject object, String key) {
        JSONObject storageInfoJson = object.getJSONObject(key);
        StorageInfoParam param = StorageInfoParam.parseJson(storageInfoJson);
        return new StorageInfo(param.getLayoutVersion(), param.getNamespaceID(),
                param.getClusterID(), param.getcTime(), HdfsServerConstants.NodeType.DATA_NODE);
    }

    public static DatanodeRegistration getDatanodeRegistration(JSONObject object, String key) {
        JSONObject datanodeRegJson = object.getJSONObject(key);
        DatanodeRegistrationParam param = DatanodeRegistrationParam.parseJson(datanodeRegJson);
        return new DatanodeRegistration(param.getDnId(), param.getStorageInfo(), param.getKeys(), param.getSoftwareVersion());
    }

    public static BlockReportContext getBlockReportContext(JSONObject object, String key) {
        JSONObject json = object.getJSONObject(key);
        BlockReportContextParam param = BlockReportContextParam.parseJson(json);
        return new BlockReportContext(param.getTotalRpcs(), param.getCurRpc(), param.getReportId());
    }

    public static StorageType getStorageType(JSONObject object, String key) {
        String type = object.getString(key);
        return StorageType.parseStorageType(type);
    }

    public static DatanodeStorage.State getDatanodeStorageState(JSONObject object, String key) {
        String state = object.getString(key);
        return DatanodeStorage.State.valueOf(state);
    }

    public static DatanodeStorage getDatanodeStorage(JSONObject object, String key) {
        JSONObject json = object.getJSONObject(key);
        DatanodeStorageParam param = DatanodeStorageParam.parseJson(json);
        return new DatanodeStorage(param.getStorageUuid(), param.getState(), param.getStorageType());
    }

    public static StorageBlockReport[] getStorageBlockReports(JSONObject object, String key) {
        JSONArray array = object.getJSONArray(key);
        StorageBlockReport[] reports = new StorageBlockReport[array.length()];
        for (int i = 0; i < array.length(); i++) {
            JSONObject jsonObject = array.getJSONObject(i);
            StorageBlockReportParam param = StorageBlockReportParam.parseJson(jsonObject);
            StorageBlockReport report = new StorageBlockReport(param.getStorage(), param.getBlocks());
            reports[i] = report;
        }
        return reports;
    }

    public static Block getBlock(JSONObject object, String key) {
        JSONObject json = object.getJSONObject(key);
        long blockID = json.getLong("blockId");
        long numBytes = json.getLong("numBytes");
        long generationStamp = json.getLong("genStamp");
        return new Block(blockID, numBytes, generationStamp);
    }

    public static ReceivedDeletedBlockInfo.BlockStatus getBlockStatus(JSONObject object, String key) {
        //TODO may recieve RECEIVING_BLOCK or DELETED_BLOCK
        //String status = object.getString(key);
        return ReceivedDeletedBlockInfo.BlockStatus.valueOf("RECEIVED_BLOCK");
    }

    public static ReceivedDeletedBlockInfo[] getReceivedDeletedBlockInfo(JSONObject object, String key) {
        JSONArray array = object.getJSONArray(key);
        ReceivedDeletedBlockInfo[] blocks = new ReceivedDeletedBlockInfo[array.length()];
        for (int i = 0; i < array.length(); i++) {
            JSONObject json = array.getJSONObject(i);
            ReceivedDeletedBlockInfoParam param = ReceivedDeletedBlockInfoParam.parseJson(json);
            blocks[i] = new ReceivedDeletedBlockInfo(param.getBlock(), param.getStatus(), param.getDelHints());
        }
        return blocks;
    }

    public static StorageReceivedDeletedBlocks[] getStorageReceivedDeletedBlocks(JSONObject object, String key) {
        JSONArray array = object.getJSONArray(key);
        StorageReceivedDeletedBlocks[] blocks = new StorageReceivedDeletedBlocks[array.length()];
        for (int i = 0; i < array.length(); i++) {
            JSONObject json = array.getJSONObject(i);
            StorageReceivedDeletedBlocksParam param = StorageReceivedDeletedBlocksParam.parseJson(json);
            blocks[i] = new StorageReceivedDeletedBlocks(param.getStorage(), param.getBlocks());
        }
        return blocks;
    }

    /*public static StorageReport getStorageReport(JSONObject object,String key){
        JSONObject json = object.getJSONObject(key);
        StorageReportParam param = StorageReportParam.parseJson(object);
        StorageReport report = new StorageReport(param.storage,
                param.failed,param.capacity,param.dfsUsed,
                param.remaining,param.blockPoolUsed);

        return report;
    }*/

    public static StorageReport[] getStorageReportList(JSONObject object, String key) {
        JSONArray array = object.getJSONArray(key);
        StorageReport[] reports = new StorageReport[array.length()];
        for (int i = 0; i < array.length(); i++) {
            JSONObject json = array.getJSONObject(i);
            StorageReportParam param = StorageReportParam.parseJson(json);
            reports[i] = new StorageReport(param.getStorage(),
                    param.isFailed(), param.getCapacity(), param.getDfsUsed(),
                    param.getRemaining(), param.getBlockPoolUsed());
        }
        return reports;
    }

    public static Priority getPriority(JSONObject object, String key) {
        JSONObject json = object.getJSONObject(key);
        return PriorityParam.parseParam(json);
    }

    public static Resource getResource(JSONObject object, String key) {
        JSONObject json = object.getJSONObject(key);
        return ResourceParam.parseParam(json);
    }

    public static List<ResourceRequest> getResourceRequestList(JSONObject object, String key) {
        if (object.has(key)) {
            JSONArray array = object.getJSONArray(key);
            List<ResourceRequest> resourceRequests = new ArrayList<>();
            for (int i = 0; i < array.length(); i++) {
                JSONObject json = array.getJSONObject(i);
                resourceRequests.add(ResourceRequestParam.parseParam(json));
            }
            return resourceRequests;
        } else {
            return null;
        }
    }

    public static FinalApplicationStatus getFinalApplicationStatus(JSONObject object, String key) {
        return FinalApplicationStatus.valueOf(object.getString(key));
    }

    public static ApplicationSubmissionContext getApplicationSubmissionContext(JSONObject object, String key) {
        JSONObject json = object.getJSONObject(key);
        return ApplicationSubmissionContextParam.parseParam(json);
    }

    public static ApplicationId getApplicationId(JSONObject object, String key) {
        JSONObject json = object.getJSONObject(key);
        return ApplicationIdParam.parseParam(json);
    }

    public static NodeId getNodeId(JSONObject object, String key) {
        JSONObject json = object.getJSONObject(key);
        return NodeIdParam.parseParam(json);
    }

    public static LocalResourceType getLocalResourceType(JSONObject object, String key) {
        String str = object.getString(key);
        return LocalResourceType.valueOf(str);
    }

    public static LocalResourceVisibility getLocalResourceVisibility(JSONObject object, String key) {
        String str = object.getString(key);
        return LocalResourceVisibility.valueOf(str);
    }

    public static URL getURL(JSONObject object, String key) {
        JSONObject json = object.getJSONObject(key);
        return URLParam.parseParam(json);
    }

    public static LocalResource getLocalResource(JSONObject object, String key) {
        JSONObject json = object.getJSONObject(key);
        return LocalResourceParam.parseParam(json);
    }

    public static Map<String, LocalResource> getLocalResources(JSONObject object, String key) {
        Map<String, LocalResource> map = new HashMap<>();
        JSONArray array = object.getJSONArray(key);
        for (int i = 0; i < array.length(); i++) {
            String _key = array.getJSONObject(i).getString("key");
            LocalResource _value = UtilJson.getLocalResource(array.getJSONObject(i), "value");
            map.put(_key, _value);
        }
        return map;
    }

    public static List<String> getStringList(JSONObject object, String key) {
        JSONArray array = object.getJSONArray(key);
        List<String> list = new ArrayList<>(array.length());
        for (int i = 0; i < array.length(); i++) {
            list.add(array.getString(i));
        }
        return list;
    }

    public static Map<String, String> getStringMap(JSONObject object, String key,
                                                   String mapKeyName, String mapValueName) {
        Map<String, String> map = new HashMap<>();
        JSONArray array = object.getJSONArray(key);
        for (int i = 0; i < array.length(); i++) {
            String _key = UtilJson.getString(array.getJSONObject(i), mapKeyName);
            String _value = UtilJson.getString(array.getJSONObject(i), mapValueName);
            map.put(_key, _value);
        }
        return map;
    }

    public static ApplicationAccessType getApplicationAccessType(JSONObject object, String key) {
        String str = object.getString(key);
        return ApplicationAccessType.valueOf(str);
    }

    public static Map<ApplicationAccessType, String> getAccessControlList(JSONObject object, String key) {
        JSONArray array = object.getJSONArray(key);
        Map<ApplicationAccessType, String> map = new HashMap<>();
        for (int i = 0; i < i; i++) {
            ApplicationAccessType _key = UtilJson.getApplicationAccessType(array.getJSONObject(i), "accessType");
            String _value = UtilJson.getString(array.getJSONObject(i), "acl");
            map.put(_key, _value);
        }
        return map;
    }

    public static NodeHealthStatus getNodeHealthStatus(JSONObject object, String key) {
        JSONObject json = object.getJSONObject(key);
        return NodeHealthStatusParam.parseParam(json);
    }

    public static NodeStatus getNodeStatus(JSONObject object, String key) {
        JSONObject json = object.getJSONObject(key);
        return NodeStatusParam.parseParam(json);
    }

    public static MasterKey getMasterKey(JSONObject object, String key) {
        JSONObject json = object.getJSONObject(key);
        return MasterKeyParam.parseParam(json);
    }

    public static ByteBuffer getByteBuffer(JSONObject object, String key) {
        String str = object.getString(key);
        if (str.contains("#")) {
            //System.out.println("getByteBuffer: " + str);
            String[] strs = str.split("#");
            byte[] bytes = new byte[strs.length];
            for (int i = 0; i < strs.length; i++) {
                bytes[i] = Byte.valueOf(strs[i]);
            }
            return ByteBuffer.wrap(bytes);
        } else {
            return ByteBuffer.wrap(DatatypeConverter.parseHexBinary(str));
        }
    }


    public static ContainerLaunchContext getContainerLaunchContext(JSONObject object, String key) {
        JSONObject json = object.getJSONObject(key);
        return ContainerLaunchContextParam.parseParam(json);
    }

    public static ApplicationAttemptId getApplicationAttemptId(JSONObject object, String key) {
        JSONObject json = object.getJSONObject(key);
        return ApplicationAttemptIdParam.parseParam(json);
    }

    public static ContainerId getContainerId(JSONObject object, String key) {
        JSONObject json = object.getJSONObject(key);
        return ContainerIdParam.parseParam(json);
    }

    public static ContainerState getContainerState(JSONObject object, String key) {
        return ContainerState.valueOf(object.getString(key));
    }

    public static List<ContainerStatus> getContainerStatuses(JSONObject object, String key) {
        if (object.has(key)) {
            JSONArray array = object.getJSONArray(key);
            List<ContainerStatus> list = new ArrayList<>(array.length());
            for (int i = 0; i < array.length(); i++) {
                JSONObject json = array.getJSONObject(i);
                list.add(ContainerStatusParam.parseParam(json));
            }
            return list;
        } else {
            return null;
        }
    }
}





















