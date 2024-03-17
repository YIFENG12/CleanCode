package com.jravity.auditadmin.controller;

import com.alibaba.excel.util.StringUtils;
import com.jravity.auditadmin.service.CloudFileService;
import com.jravity.auditadmin.service.SyncDataService;
import com.jravity.utils.model.ResponseBean;
import com.jravity.utils.model.req.FileMultipartyInfoReq;
import com.jravity.utils.model.req.MergeFileReq;
import com.jravity.utils.model.req.VerifyFileReq;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Mono;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 云盘文件上传
 * 此类的接口部署多分时，需要映射到同一个磁盘中
 * 或者 nginx转发时用ip_hash策略，
 * 否则分片文件会上传到不同的地方
 *
 * @author changfeng
 * @date 2022/9/13
 */
@Slf4j
@RestController
@AllArgsConstructor
@RequestMapping("/cloudFile")
public class CloudFileController {

    private CloudFileService cloudFileService;
    private SyncDataService syncDataServiceImpl;

    private static class Range {
        long start;
        long end;
        long length;
        long total;

        /**
         * Range段构造方法.
         *
         * @param start range起始位置.
         * @param end   range结束位置.
         * @param total range段的长度.
         */
        public Range(long start, long end, long total) {
            this.start = start;
            this.end = end;
            this.length = end - start + 1;
            this.total = total;
        }

        public static long sublong(String value, int beginIndex, int endIndex) {
            String substring = value.substring(beginIndex, endIndex);
            return (substring.length() > 0) ? Long.parseLong(substring) : -1;
        }

        private static void copy(RandomAccessFile randomAccessFile, OutputStream output, long fileSize, long start, long length) throws IOException {
            byte[] buffer = new byte[4096];
            int read = 0;
            long transmitted = 0;
            if (fileSize == length) {
                randomAccessFile.seek(start);
                //需要下载的文件长度与文件长度相同，下载整个文件.
                while ((transmitted + read) <= length && (read = randomAccessFile.read(buffer)) != -1){
                    output.write(buffer, 0, read);
                    transmitted += read;
                }
                //处理最后不足buff大小的部分
                if(transmitted < length){
                    log.info("最后不足buff大小的部分大小为：" + (length - transmitted));
                    read = randomAccessFile.read(buffer, 0, (int)(length - transmitted));
                    output.write(buffer, 0, read);
                }
            } else {
                randomAccessFile.seek(start);
                long toRead = length;

                //如果需要读取的片段，比单次读取的4096小，则使用读取片段大小读取
                if(toRead < buffer.length){
                    output.write(buffer, 0, randomAccessFile.read(new byte[(int) toRead]));
                    return;
                }
                while ((read = randomAccessFile.read(buffer)) > 0) {
                    toRead -= read;
                    if (toRead > 0) {
                        output.write(buffer, 0, read);
                    } else {
                        output.write(buffer, 0, (int) toRead + read);
                        break;
                    }
                }

            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Range range = (Range) o;
            return start == range.start &&
                    end == range.end &&
                    length == range.length &&
                    total == range.total;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            return Objects.hash(prime, start, end, length, total);
        }
    }


    /**
     * 文件下载
     * @author kevin
     * @param response :
     * @param range :
     * @param filePath :
     * @date 2021/1/17
     */
    @GetMapping(value = "/downloadFile")
    public void downloadFile(@RequestParam("fileId") String fileId, @RequestParam(name = "filePath",
            required = false) String filePath, HttpServletResponse response,
                             @RequestHeader(name = "Range", required = false) String range) {

        List<FileInfo> fileInfo= fileMapper.getFileById(fileId);
        if(null == fileInfo){
            throw new RuntimeException("下载失败，未找到需要下载的文件");
        }
        filePath = StringUtils.isNotBlank(filePath) ? filePath : fileInfo.getFilePath();

        File file = new File(filePath);
        String filename = file.getName();
        long length = file.length();
        Range full = new Range(0, length - 1, length);
        List<Range> ranges = new ArrayList<>();
        //处理Range
        try {
            if (!file.exists()) {
                String msg = "需要下载的文件不存在：" + file.getAbsolutePath();
                log.error(msg);
                throw new RuntimeException(msg);
            }

            if (file.isDirectory()) {
                String msg = "需要下载的文件的路径对应的是一个文件夹：" + file.getAbsolutePath();
                log.error(msg);
                throw new RuntimeException("ResponseState.REQUEST_ERROR.getCode(), msg");
            }
            dealRanges(full, range, ranges, response, length);
        }catch (IOException e){
            e.printStackTrace();
            throw new RuntimeException("文件下载异常：" + e.getMessage());
        }
        // 如果浏览器支持内容类型，则设置为“内联”，否则将弹出“另存为”对话框. attachment inline
        String disposition = "attachment";

        // 将需要下载的文件段发送到客服端，准备流.
        try (RandomAccessFile input = new RandomAccessFile(file, "r");
             ServletOutputStream output = response.getOutputStream()) {
            //最后修改时间
            FileTime lastModifiedObj = Files.getLastModifiedTime(file.toPath());
            long lastModified = LocalDateTime.ofInstant(lastModifiedObj.toInstant(),
                    ZoneId.of(ZoneId.systemDefault().getId())).toEpochSecond(ZoneOffset.UTC);
            //初始化response.
            response.reset();
            response.setBufferSize(20480);
            response.setHeader("Content-type", "application/octet-stream;charset=UTF-8");
            response.setHeader("Content-Disposition", disposition + ";filename=" +
                    URLEncoder.encode(filename, StandardCharsets.UTF_8.name()));
            response.setHeader("Accept-Ranges", "bytes");
            response.setHeader("ETag", URLEncoder.encode(filename, StandardCharsets.UTF_8.name()));
            response.setDateHeader("Last-Modified", lastModified);
            response.setDateHeader("Expires", System.currentTimeMillis() + 604800000L);
            //输出Range到response
            outputRange(response, ranges, input, output, full, length);
            output.flush();
            response.flushBuffer();
        }catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException("文件下载异常：" + e.getMessage());
        }
    }

    /**
     * 处理请求中的Range(多个range或者一个range，每个range范围)
     * @author kevin
     * @param range :
     * @param ranges :
     * @param response :
     * @param length :
     * @date 2021/1/17
     */
    private void dealRanges(Range full, String range, List<Range> ranges, HttpServletResponse response,
                            long length) throws IOException {
        if (range != null) {
            // Range 头的格式必须为 "bytes=n-n,n-n,n-n...". 如果不是此格式, 返回 416.
            if (!range.matches("^bytes=\\d*-\\d*(,\\d*-\\d*)*$")) {
                response.setHeader("Content-Range", "bytes */" + length);
                response.sendError(HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE);
                return;
            }

            // 处理传入的range的每一段.
            for (String part : range.substring(6).split(",")) {
                part = part.split("/")[0];
                // 对于长度为100的文件，以下示例返回:
                // 50-80 (50 to 80), 40- (40 to length=100), -20 (length-20=80 to length=100).
                int delimiterIndex = part.indexOf("-");
                long start = Range.sublong(part, 0, delimiterIndex);
                long end = Range.sublong(part, delimiterIndex + 1, part.length());

                //如果未设置起始点，则计算的是最后的 end 个字节；设置起始点为 length-end，结束点为length-1
                //如果未设置结束点，或者结束点设置的比总长度大，则设置结束点为length-1
                if (start == -1) {
                    start = length - end;
                    end = length - 1;
                } else if (end == -1 || end > length - 1) {
                    end = length - 1;
                }

                // 检查Range范围是否有效。如果无效，则返回416.
                if (start > end) {
                    response.setHeader("Content-Range", "bytes */" + length);
                    response.sendError(HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE);
                    return;
                }
                // 添加Range范围.
                ranges.add(new Range(start, end, end - start + 1));
            }
        }else{
            //如果未传入Range，默认下载整个文件
            ranges.add(full);
        }
    }



    /**
     * output写流输出到response
     * @author kevin
     * @param response :
     * @param ranges :
     * @param input :
     * @param output :
     * @param full :
     * @param length :
     * @date 2021/1/17
     */
    private void outputRange(HttpServletResponse response, List<Range> ranges, RandomAccessFile input,
                             ServletOutputStream output, Range full, long length) throws IOException {
        if (ranges.isEmpty() || ranges.get(0) == full) {
            // 返回整个文件.
            response.setContentType("application/octet-stream;charset=UTF-8");
            response.setHeader("Content-Range", "bytes " + full.start + "-" + full.end + "/" + full.total);
            response.setHeader("Content-length", String.valueOf(full.length));
            response.setStatus(HttpServletResponse.SC_OK); // 200.
            Range.copy(input, output, length, full.start, full.length);
        } else if (ranges.size() == 1) {
            // 返回文件的一个分段.
            Range r = ranges.get(0);
            response.setContentType("application/octet-stream;charset=UTF-8");
            response.setHeader("Content-Range", "bytes " + r.start + "-" + r.end + "/" + r.total);
            response.setHeader("Content-length", String.valueOf(r.length));
            response.setStatus(HttpServletResponse.SC_PARTIAL_CONTENT); // 206.
            // 复制单个文件分段.
            Range.copy(input, output, length, r.start, r.length);
        } else {
            // 返回文件的多个分段.
            response.setContentType("multipart/byteranges; boundary=MULTIPART_BYTERANGES");
            response.setStatus(HttpServletResponse.SC_PARTIAL_CONTENT); // 206.

            // 复制多个文件分段.
            for (Range r : ranges) {
                //为每个Range添加MULTIPART边界和标题字段
                output.println();
                output.println("--MULTIPART_BYTERANGES");
                output.println("Content-Type: application/octet-stream;charset=UTF-8");
                output.println("Content-length: " + r.length);
                output.println("Content-Range: bytes " + r.start + "-" + r.end + "/" + r.total);
                // 复制多个需要复制的文件分段当中的一个分段.
                Range.copy(input, output, length, r.start, r.length);
            }

            // 以MULTIPART文件的边界结束.
            output.println();
            output.println("--MULTIPART_BYTERANGES--");
        }
    }



    /**
     * 分片上传(临时文件会在SchedulerChunkCleanConfig中删除)
     */
    @PostMapping(value = "fileUploadMultiparty")
    public Mono<ResponseBean<Boolean>> fileUploadMultiparty(@RequestBody MultipartFile file, @RequestHeader("sign-check") String check, FileMultipartyInfoReq req) {
        log.info("分片上传, req: {}", req);
        Boolean checkFlag = syncDataServiceImpl.signCheck(check, req.getSysCode());
        if (!checkFlag) {
            return Mono.just(new ResponseBean<>(HttpStatus.UNAUTHORIZED.value(), false, "签名错误", null));
        }

        req.setChunk(file);
        return Mono.just(ResponseBean.success(cloudFileService.fileUploadMultiparty(req)));
    }

    /**
     * 校验分片文件，返回已存在的分片名
     */
    @PostMapping(value = "verify")
    public Mono<ResponseBean<List<String>>> verify(@RequestHeader("sign-check") String check, @RequestBody VerifyFileReq req) {
        log.info("校验分片文件，返回已存在的分片名, req: {}", req);
        Boolean checkFlag = syncDataServiceImpl.signCheck(check, req.getSysCode());
        if (!checkFlag) {
            return Mono.just(new ResponseBean<>(HttpStatus.UNAUTHORIZED.value(), false, "签名错误", null));
        }
        return Mono.just(ResponseBean.success(cloudFileService.verify(req)));
    }

    /**
     * 合并分片文件，返回合并后的文件url
     */
    @PostMapping(value = "mergeFile")
    public Mono<ResponseBean<String>> mergeFile(@RequestHeader("sign-check") String check, @RequestBody MergeFileReq req) {
        log.info("合并分片文件，返回合并后的文件url, req: {}", req);
        Boolean checkFlag = syncDataServiceImpl.signCheck(check, req.getSysCode());
        if (!checkFlag) {
            return Mono.just(new ResponseBean<>(HttpStatus.UNAUTHORIZED.value(), false, "签名错误", null));
        }

        try {
            return Mono.just(ResponseBean.success(cloudFileService.mergeFile(req)));
        } catch (Exception e) {
            log.error("合并分片文件异常，请求信息{}", req, e);
            return Mono.just(ResponseBean.failed(500, e.getLocalizedMessage()));
        }

    }
}
