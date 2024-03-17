package com.jravity.auditadmin.service.impl;

import com.jravity.auditadmin.config.property.TxAsrProperties;
import com.jravity.auditadmin.service.CloudFileService;
import com.jravity.auditadmin.executor.TxAsrClientExecutor;
import com.jravity.mongo.audit.TxAsrTask;
import com.jravity.mongo.audit.constants.SourceIdTypeEnum;
import com.jravity.utils.constants.enums.BizExceptionEnum;
import com.jravity.utils.exception.BizException;
import com.jravity.utils.model.req.FileMultipartyInfoReq;
import com.jravity.utils.model.req.MergeFileReq;
import com.jravity.utils.model.req.VerifyFileReq;
import com.tencentcloudapi.asr.v20190614.models.CreateRecTaskResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

import static com.jravity.auditadmin.util.file.AdminFileUtil.TEMP_DIR;

/**
 * 云盘文件上传
 *
 * @author changfeng
 * @date 2022/9/13
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CloudFileServiceImpl implements CloudFileService {

    private final TxAsrClientExecutor txAsrClientExecutor;
    private final MongoTemplate mongoTemplate;
    private final TxAsrProperties txAsrProperties;

    @Override
    public Boolean fileUploadMultiparty(FileMultipartyInfoReq req) {
        final MultipartFile file = req.getChunk();
        final long fileSize = file.getSize();
        final String fileId = req.getId();
        final String chunkName = req.getChunkName();
        log.info("文件上传开始，fileMd5{},chunkName{}", fileId, chunkName);
        final String dirPath = getTempDirPath(fileId, req.getSysCode(), req.getFileType());
        File dirFile = new File(dirPath);
        if (!dirFile.exists() && !dirFile.mkdirs()) {
            log.error("创建文件夹【{}】失败，请检查目录权限！", dirPath);
        }
        final String chunkFileName = dirPath + File.separator + chunkName;
        final File chunkFile = new File(chunkFileName);

        try (InputStream in = file.getInputStream();
             OutputStream out = new FileOutputStream(chunkFile)) {
            StreamUtils.copy(in, out);
            log.info("文件上传完毕，fileMd5{},chunkName{}", fileId, chunkName);
        } catch (IOException e) {
            log.error("文件上传失败", e);
        }
        if (chunkFile.exists() && (fileSize == chunkFile.length())) {
            return Boolean.TRUE;
        }
        log.error("文件上传失败,fileMd5{},chunkName{},文件大小:{},保存大小{}",
                fileId, chunkName, fileSize, chunkFile.length());
        chunkFile.delete();
        return Boolean.FALSE;

    }

    @Override
    public String mergeFile(MergeFileReq req) {
        final String fileId = req.getId();
        final String fileName = req.getFileName();
        final String dirPath = getTempDirPath(fileId, req.getSysCode(), req.getFileType());
        File dirFile = new File(dirPath);
        if (!dirFile.exists() && StringUtils.isBlank(fileName)) {
            log.error("文件名为空");
            throw new RuntimeException("文件名为空");
        }
        if (!dirFile.exists()) {
            log.error("文件夹不存在，{}", dirPath);
            throw new RuntimeException("文件夹不存在");
        }
        final File[] files = dirFile.listFiles();
        if (files.length != req.getTotalLength()) {
            log.error("分片缺失，{}, length:{},getTotalLength:{}", dirPath, files.length, req.getTotalLength());
            return "";
        }
        // 排序
        final List<File> collect = Arrays.stream(files)
                .sorted(Comparator.comparing(File::getName,
                        (s1, s2) -> s1.length() != s2.length()
                                ? Integer.compare(s1.length(), s2.length()) : s1.compareTo(s2)))
                .collect(Collectors.toList());

        // 完整文件
        final File file = new File(dirPath + File.separator + fileName);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                log.error("创建文件失败,{}", file.getAbsolutePath(), e);
            }
        }
        try (final RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            byte[] temp = new byte[TxAsrProperties.BYTE_SIZE];
            // 合并文件
            for (File f : collect) {
                try (FileInputStream fin = new FileInputStream(f)) {
                    randomAccessFile.skipBytes(0);
                    int i;
                    while ((i = fin.read(temp)) != -1) {
                        randomAccessFile.write(temp, 0, i);
                    }
                } catch (FileNotFoundException e) {
                    log.error("找不到文件：{}", f.getAbsolutePath());
                    throw e;
                }
            }

            // 合并完后，删除分片文件
            collect.stream().filter(File::exists)
                    .forEach(fileItem -> fileItem.delete());
            dirFile.delete();

            // 开启语音识别
            String viewUrl = txAsrProperties.getViewFileBaseAddress() + file.getPath().split(TEMP_DIR)[1];
            if (Objects.nonNull(req.getSpeechRecognition()) && req.getSpeechRecognition()) {
                // 返回的resp是一个CreateRecTaskResponse的实例，与请求对象对应
                CreateRecTaskResponse resp = txAsrClientExecutor.createRecTask(viewUrl);
                if (resp.getData().getTaskId() == null || resp.getRequestId() == null) {
                    throw new BizException(BizExceptionEnum.CREATE_ASR_ERROR);
                }
                // 结果保存映射
                TxAsrTask txAsrTask = new TxAsrTask();
                txAsrTask.setSourceId(req.getFileHistoryId());
                txAsrTask.setSourceIdType(SourceIdTypeEnum.tbl_cloud_file_history);
                txAsrTask.setSysCode(req.getSysCode());
                txAsrTask.setUrl(viewUrl);
                txAsrTask.setTaskId(resp.getData().getTaskId());
                txAsrTask.setRequestId(resp.getRequestId());
                txAsrTask.setFilePath(file.getPath());
                mongoTemplate.save(txAsrTask);
            }

            return viewUrl;
        } catch (Exception e) {
            log.error("合并文件异常,", e);
            throw new RuntimeException("合并文件异常");
        }
    }

    @Override
    public List<String> verify(VerifyFileReq req) {

        final String fileId = req.getId();
        final String dirPath = getTempDirPath(fileId, req.getSysCode(), req.getFileType());
        final File file = new File(dirPath);
        if (file.isDirectory()) {
            final String[] files = file.list();
            if (files != null) {
                return Arrays.asList(files);
            }
        }
        return Collections.emptyList();
    }

    private String getTempDirPath(String fileMd5, String sysCode, String fileType) {
        return TEMP_DIR + fileType + File.separator + sysCode + File.separator + fileMd5;
    }
}
