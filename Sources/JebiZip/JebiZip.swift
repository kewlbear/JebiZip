//
//  JebiZip.swift
//
//  Copyright (c) 2019 Changbeom Ahn
//
//  Permission is hereby granted, free of charge, to any person obtaining a copy
//  of this software and associated documentation files (the "Software"), to deal
//  in the Software without restriction, including without limitation the rights
//  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//  copies of the Software, and to permit persons to whom the Software is
//  furnished to do so, subject to the following conditions:
//
//  The above copyright notice and this permission notice shall be included in
//  all copies or substantial portions of the Software.
//
//  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
//  THE SOFTWARE.
//

import Foundation
import zlib
#if canImport(Compression)
import Compression

extension compression_stream {
    init() {
        self = UnsafeMutablePointer<compression_stream>.allocate(capacity: 1).pointee
    }
}

@available(macOS 10.11, *)
class Compression: Decompressor {
    lazy var destinationBufferPointer = UnsafeMutablePointer<UInt8>.allocate(capacity: bufferSize)
    
    lazy var scratchBufferPointer = UnsafeMutablePointer<UInt8>.allocate(capacity: compression_decode_scratch_buffer_size(COMPRESSION_ZLIB))
    
    deinit {
        scratchBufferPointer.deallocate()
        destinationBufferPointer.deallocate()
    }

    func decompress(input: (Int) -> Data, compressedSize: Int, decompressedSize: Int, output: (Result<Data, Error>) throws -> Void) throws {
        guard decompressedSize > bufferSize || compressedSize > 1024 * 1024 else {
            let data = input(compressedSize)
            try output(.success(data.withUnsafeBytes {
                let baseAddress = $0.bindMemory(to: UInt8.self).baseAddress!
                let count = compression_decode_buffer(destinationBufferPointer, bufferSize, baseAddress, data.count, scratchBufferPointer, COMPRESSION_ZLIB)
                return Data(bytesNoCopy: destinationBufferPointer,
                                      count: count,
                                      deallocator: .none)
            }))
            return
        }
        
        var stream = compression_stream()
        var status = compression_stream_init(&stream, COMPRESSION_STREAM_DECODE, COMPRESSION_ZLIB)
        guard status != COMPRESSION_STATUS_ERROR else {
            fatalError("Unable to initialize the compression stream.")
        }
        defer {
            compression_stream_destroy(&stream)
        }
        
        stream.src_size = 0
        stream.dst_ptr = destinationBufferPointer
        stream.dst_size = bufferSize
        
        var sourceData: Data?
        repeat {
            var flags = Int32(0)
            
            // If this iteration has consumed all of the source data,
            // read a new tempData buffer from the input file.
            if stream.src_size == 0 {
                sourceData = input(bufferSize)
                
                stream.src_size = sourceData!.count
                if sourceData!.count < bufferSize {
                    flags = Int32(COMPRESSION_STREAM_FINALIZE.rawValue)
                }
            }
        
            if let sourceData = sourceData {
                let count = sourceData.count
                
                sourceData.withUnsafeBytes {
                    let baseAddress = $0.bindMemory(to: UInt8.self).baseAddress!
                    
                    stream.src_ptr = baseAddress.advanced(by: count - stream.src_size)
                    status = compression_stream_process(&stream, flags)
                }
            }
            
            switch status {
            case COMPRESSION_STATUS_OK,
                 COMPRESSION_STATUS_END:
                
                // Get the number of bytes put in the destination buffer. This is the difference between
                // stream.dst_size before the call (here bufferSize), and stream.dst_size after the call.
                let count = bufferSize - stream.dst_size
                
                let outputData = Data(bytesNoCopy: destinationBufferPointer,
                                      count: count,
                                      deallocator: .none)
                
                // Write all produced bytes to the output file.
                try output(.success(outputData))
                
                // Reset the stream to receive the next batch of output.
                stream.dst_ptr = destinationBufferPointer
                stream.dst_size = bufferSize
            default:
                fatalError() // FIXME: ...
            }
        } while status == COMPRESSION_STATUS_OK
    }
}
#endif

private let ErrorDomain = "JebiZipErrorDomain"

public enum JebiZipError: Error {
    case noError
    case invalidSignature
    case corruptFile
    case invalidStringEncoding
    case zlib // TODO: properly handle zlib errors
    case unsupportedCompressionMethod
    case notZip64
    case invalidState // logic error
}

extension JebiZipError: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case .noError:
            fatalError()
        case .invalidSignature:
            return "Invalid signature"
        case .corruptFile:
            return "corrupt file"
        case .invalidStringEncoding:
            return "invalid string encoding"
        case .zlib:
            return "zlib error"
        case .unsupportedCompressionMethod:
            return "unsupported compression method"
        case .notZip64:
            return "not Zip64"
        case .invalidState:
            return "invalid state"
        }
    }
}

public struct Version: CustomStringConvertible {
    
    public enum Host: UInt8 {
        case msDos, amiga, openVms, unix, vmCms,
        atariSt, os2Hpfs, macintosh, zSystem, cpM,
        windowsNtfs, mvs, vse, acornRisc, vfat,
        alternateMvs, beOs, tandem, os400, macOS,
        invalid
    }
    
    public var host: Host {
        return Host(rawValue: UInt8(value >> 8)) ?? .invalid
    }
    
    public var major: Int {
        return Int((value & 0xff) / 10)
    }
    
    public var minor: Int {
        return Int((value & 0xff) % 10)
    }
    
    public var description: String {
        return "\(major).\(minor) \(host)"
    }
    
    private let value: UInt16
    
    init(_ value: UInt16) {
        self.value = value
    }

}

public enum CompressionMethod: UInt16 {
    case store, shrink, reduce1, reduce2, reduce3, reduce4, implode, tokenize,
    deflate, deflate64, terseOld,
    bzip2 = 12,
    lzma = 14,
    cmpsc = 16,
    terse = 18, lz77,
    jpegVariant = 96, wavPack, ppmd, ae_x,
    invalid
}

public protocol ZipEntry {
    
    var filename: String { get }

    func write(to: URL) throws
    
}

protocol Reader {
    var zipFile: FileHandle? { get }
    func readString(ofLength: Int) throws -> String
    func readUInt16() throws -> UInt16
    func readUInt32() throws -> UInt32
    func readUInt64() throws -> UInt64
    func readData(ofLength: Int) throws -> Data
}

var chunk = 128 * 1024

let bufferSize = 8 * 1024 * 1024

protocol Decompressor {
    func decompress(input: (Int) -> Data, compressedSize: Int, decompressedSize: Int, output: (Result<Data, Error>) throws -> Void) throws
}

class Zlib: Decompressor {
    lazy var out = Array(repeating: UInt8(0), count: chunk)

    func decompress(input: (Int) -> Data, compressedSize: Int, decompressedSize: Int, output: (Result<Data, Error>) throws -> Void) throws {
        var remain = Int(compressedSize)
        var stream = z_stream()
        stream.zalloc = nil
        stream.zfree = nil
        stream.opaque = nil
        stream.avail_in = 0
        stream.next_in = nil
        guard inflateInit2_(&stream, -MAX_WBITS, ZLIB_VERSION, Int32(MemoryLayout<z_stream>.size)) == Z_OK else {
            throw JebiZipError.zlib
        }
        
        defer {
            inflateEnd(&stream)
        }
        var ret = Int32(0)
        
        repeat {
            try autoreleasepool {
                var data = input(min(chunk, remain))
//                        print("read", data)//.map { String($0, radix: 16, uppercase: false) })
                remain -= data.count
                stream.avail_in = uInt(data.count)
                try data.withUnsafeMutableBytes {
                    stream.next_in = $0.bindMemory(to: UInt8.self).baseAddress
                    
                    repeat {
                        try out.withUnsafeMutableBytes {
                            stream.avail_out = uInt(chunk)
                            stream.next_out = $0.bindMemory(to: UInt8.self).baseAddress
                            ret = zlib.inflate(&stream, Z_NO_FLUSH)
                            switch ret {
                            case Z_NEED_DICT:
                                throw JebiZipError.zlib
                            case Z_DATA_ERROR:
                                throw JebiZipError.zlib
                            case Z_MEM_ERROR:
                                throw JebiZipError.zlib
                            default:
//                                        print("inflate() =", ret)
                                break
                            }
                            
                            // TODO: write
//                                    print("write", chunk - Int(stream.avail_out))
                            try $0.baseAddress.map {
                                let data = Data(bytesNoCopy: $0, count: chunk - Int(stream.avail_out), deallocator: .none)
                                try output(.success(data))
                            }
                        }
                    } while stream.avail_out == 0
                }
            }
        } while ret != Z_STREAM_END &&
            remain > 0
        
        assert(remain == 0)
    }
}

@available(iOS 9.0, macOS 10.11, *)
open class Zip {
    
    private struct Entry: ZipEntry {
        let versionNeededToExtract: UInt16
        let flags: UInt16
        let compressionMethod: CompressionMethod
        let lastModificationTime: UInt16
        let lastModificationDate: UInt16
        let crc32: UInt32
        var compressedSize: UInt32
        let uncompressedSize: UInt32
        let filenameLength: UInt16
        let extraFieldLength: UInt16
        let filename: String
        let extra: Data
        
        private let reader: Reader
        
        private let offset: UInt64
        
        let decompressor: Decompressor
        
        init(reader: Reader, decompressor: Decompressor) throws {
            self.reader = reader
            self.decompressor = decompressor
        
            versionNeededToExtract = try reader.readUInt16()
            flags = try reader.readUInt16()
            compressionMethod = CompressionMethod(rawValue: try reader.readUInt16()) ?? .invalid
            lastModificationTime = try reader.readUInt16()
            lastModificationDate = try reader.readUInt16()
            crc32 = try reader.readUInt32()
            compressedSize = try reader.readUInt32()
            uncompressedSize = try reader.readUInt32()
            filenameLength = try reader.readUInt16()
            extraFieldLength = try reader.readUInt16()
            filename = try reader.readString(ofLength: Int(filenameLength))
            extra = try reader.readData(ofLength: Int(extraFieldLength))
//            print("file", filename, "compression:", compressionMethod)//, "date:", lastModificationDate, "extra: ", extra.map { $0 }, "flags:", flags)

            offset = reader.zipFile?.offsetInFile ?? 0
        }
        
        func write(to url: URL) throws {
            do {
                let directory = url.deletingLastPathComponent()
                try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true, attributes: nil)
//                print("created directory:", directory.path)

                try Data().write(to: url)
//                print("created file:", url.path)
                
                let writer = try FileHandle(forWritingTo: url)
                
                try enumerateChunks(decompressor: decompressor) {
                    let data = try $0.get()
                    writer.write(data)
                }
            }
            catch {
                let underlyingError = (error as NSError).userInfo[NSUnderlyingErrorKey] as? NSError
//                guard underlyingError?.domain == NSPOSIXErrorDomain && underlyingError?.code == kPOSIXErrorEISDIR - kPOSIXErrorBase else {
//                    print(error)
                    throw error
//                }
//                print("ignored", error)
            }
        }
        
        func enumerateChunks(decompressor: Decompressor, using handler: (Result<Data, Error>) throws -> Void) throws {
            switch compressionMethod {
            case .store:
                try read(using: handler)
            case .deflate:
                try inflate(decompressor: decompressor, using: handler)
            default:
                throw JebiZipError.unsupportedCompressionMethod
            }
        }
        
        func read(using handler: (Result<Data, Error>) throws -> Void) throws {
            var remain = Int(compressedSize)
            
            while remain > 0 {
                guard let data = reader.zipFile?.readData(ofLength: min(chunk, remain)) else {
                    throw JebiZipError.corruptFile
                }
                remain -= data.count
                
                try handler(.success(data))
            }
        }
        
        func inflate(decompressor: Decompressor, using handler: (Result<Data, Error>) throws -> Void) throws {
//            print("compressed:", compressedSize, "uncompressed:", uncompressedSize)
            try reader.zipFile.map { fileHandle in
                try decompressor.decompress(input: { count in
                    fileHandle.readData(ofLength: count)
                }, compressedSize: Int(compressedSize), decompressedSize: Int(uncompressedSize), output: handler)
            }
        }
        
    }
    
    private struct CentralDirectoryHeader: ZipEntry {
        
        static let signature = UInt32(0x02014b50)
        
        let versionMadeBy: UInt16
        let versionNeededToExtract: UInt16
        let flags: UInt16
        let compressionMethod: UInt16
        let lastModificationTime: UInt16 // https://docs.microsoft.com/ko-kr/windows/desktop/api/winbase/nf-winbase-dosdatetimetofiletime
        let lastModificationDate: UInt16
        let crc32: UInt32
        let compressedSize: UInt32
        let uncompressedSize: UInt32
        let filenameLength: UInt16
        let extraFieldLength: UInt16
        let fileCommentLength: UInt16
        let diskNumberStart: UInt16
        let internalFileAttributes: UInt16
        let externalFileAttributes: UInt32 // https://docs.microsoft.com/ko-kr/windows/desktop/FileIO/file-attribute-constants
        let relativeOffsetOfLocalHeader: UInt32
        let filename: String
        let extraField: Data
        let fileComment: String
        
        let reader: Reader
        
        let decompressor: Decompressor
        
        init(reader: Reader, decompressor: Decompressor) throws {
            self.reader = reader
            self.decompressor = decompressor
            
            versionMadeBy = try reader.readUInt16()
            versionNeededToExtract = try reader.readUInt16()
            flags = try reader.readUInt16()
            compressionMethod = try reader.readUInt16()
            lastModificationTime = try reader.readUInt16()
            lastModificationDate = try reader.readUInt16()
            crc32 = try reader.readUInt32()
            compressedSize = try reader.readUInt32()
            uncompressedSize = try reader.readUInt32()
            filenameLength = try reader.readUInt16()
            extraFieldLength = try reader.readUInt16()
            fileCommentLength = try reader.readUInt16()
            diskNumberStart = try reader.readUInt16()
            internalFileAttributes = try reader.readUInt16()
            externalFileAttributes = try reader.readUInt32()
            relativeOffsetOfLocalHeader = try reader.readUInt32()
            filename = try reader.readString(ofLength: Int(filenameLength))
            extraField = try reader.readData(ofLength: Int(extraFieldLength))
            fileComment = try reader.readString(ofLength: Int(fileCommentLength))
            
//            print(filename, String(externalFileAttributes, radix: 16, uppercase: false))
        }
        
        func write(to url: URL) throws {
            guard let fileHandle = reader.zipFile else {
                throw JebiZipError.invalidState
            }
            let offset = fileHandle.offsetInFile

            fileHandle.seek(toFileOffset: UInt64(relativeOffsetOfLocalHeader))
            let localFileHeaderSignature = "PK\u{03}\u{04}"
            let signature = try reader.readString(ofLength: localFileHeaderSignature.count)
            guard signature == localFileHeaderSignature else {
                throw JebiZipError.corruptFile
            }

            var entry = try Entry(reader: reader, decompressor: decompressor)
            
            entry.compressedSize = compressedSize // TODO: better way?
            
            try entry.write(to: url)

            fileHandle.seek(toFileOffset: offset)
        }
        
    }
    
    private struct Zip64EndOfCentralDirectory {
        static let signature = UInt32(0x06064b50)
        
        let size: UInt64
        let versionMadeBy: UInt16
        let versionNeeded: UInt16
        let diskNumber: UInt32
        let diskNumberCentralDirectory: UInt32
        let entryCount: UInt64
        let totalEntryCount: UInt64
        let centralDirectorySize: UInt64
        let offset: UInt64
        let extensibleData: Data
        
        init(reader: Reader) throws {
            size = try reader.readUInt64()
            versionMadeBy = try reader.readUInt16()
            versionNeeded = try reader.readUInt16()
            diskNumber = try reader.readUInt32()
            diskNumberCentralDirectory = try reader.readUInt32()
            entryCount = try reader.readUInt64()
            totalEntryCount = try reader.readUInt64()
            centralDirectorySize = try reader.readUInt64()
            offset = try reader.readUInt64()
            let fixed = UInt64(
                44
//                2 + 2 + 4 + 4 + 8 + 8 + 8 + 8
            )
            extensibleData = try reader.readData(ofLength: Int(size - fixed))
        }
    }
    
    private struct Zip64EndOfCentralDirectoryLocator {
        static let signature = UInt32(0x07064b50)
        
        let diskNumber: UInt32
        let offset: UInt64
        let diskCount: UInt32
        
        init(reader: Reader) throws {
            diskNumber = try reader.readUInt32()
            offset = try reader.readUInt64()
            diskCount = try reader.readUInt32()
        }
    }
    
    private struct EndOfCentralDirectory {
        static let signature = UInt32(0x06054b50)
        
        let numberOfThisDisk: UInt16
        let numberOfDiskWithStartOfCentralDirectory: UInt16
        let numberOfEntriesInCentralDirectoryThisDisk: UInt16
        let totalNumberOfEntriesInCentralDirectory: UInt16
        let centralDirectorySize: UInt32
        let centralDirectoryStartOffset: UInt32
        let zipFileCommentLength: UInt16
        let zipFileComment: String
        
        var isZip64: Bool {
            return centralDirectoryStartOffset == .max // TODO: || ...
        }
        
        let reader: Reader
        let offset: UInt64
        
        init(reader: Reader) throws {
            self.reader = reader
            offset = reader.zipFile?.offsetInFile ?? .max
            
            numberOfThisDisk = try reader.readUInt16()
            numberOfDiskWithStartOfCentralDirectory = try reader.readUInt16()
            numberOfEntriesInCentralDirectoryThisDisk = try reader.readUInt16()
            totalNumberOfEntriesInCentralDirectory = try reader.readUInt16()
            centralDirectorySize = try reader.readUInt32()
            centralDirectoryStartOffset = try reader.readUInt32()
            zipFileCommentLength = try reader.readUInt16()
            zipFileComment = try reader.readString(ofLength: Int(zipFileCommentLength))
        }
    }
    
    private let url: URL
    
    internal var zipFile: FileHandle?
    
    private var enumerator: ((Result<ZipEntry, Error>) throws -> Void)?
    
    var decompressor: Decompressor
    
    public init(url: URL) {
        self.url = url
        decompressor = Zlib()
#if canImport(Compression)
        decompressor = Compression()
#endif
    }
    
    open func extract(to url: URL) throws {
        // TODO: ...
        try enumerateEntries {
            let entry = try $0.get()
            let isDirectory = entry.filename.hasSuffix("/") // TODO: external attributes?
            let entryUrl = URL(fileURLWithPath: entry.filename, isDirectory: isDirectory, relativeTo: url)
            if isDirectory {
                do {
                    try FileManager.default.removeItem(at: entryUrl)
//                    print("removed", entryUrl.path)
                }
                catch {
                    let underlyingError = (error as NSError).userInfo[NSUnderlyingErrorKey] as? POSIXError
                    guard underlyingError?.code == .ENOENT else { throw error }
//                    print("ignored", error)
                }
                try FileManager.default.createDirectory(at: entryUrl, withIntermediateDirectories: true, attributes: nil) // TODO: attributes
//                print("created", entryUrl.path)
            } else {
                try entry.write(to: entryUrl)
            }
        }
    }

    open func enumerateEntries(using handler: @escaping (Result<ZipEntry, Error>) throws -> Void) rethrows {
        self.enumerator = handler
        do {
            try read()
        }
        catch {
            try handler(.failure(error))
        }
    }
    
    private func read() throws {
        zipFile = try FileHandle(forReadingFrom: url)
        
        let endOfCentralDirectory = try findEndOfCentralDirectory()
        
        assert(endOfCentralDirectory.numberOfThisDisk == endOfCentralDirectory.numberOfDiskWithStartOfCentralDirectory)
        if endOfCentralDirectory.isZip64 {
            let locator = try findZip64EndOfCentralDirectoryLocator(endOfCentralDirectory: endOfCentralDirectory)
            assert(locator.diskCount == 1)
            zipFile?.seek(toFileOffset: locator.offset)
            guard try readUInt32() == Zip64EndOfCentralDirectory.signature else {
                throw JebiZipError.corruptFile
            }
            let endOfCentralDirectory = try Zip64EndOfCentralDirectory(reader: self)
//            print("central directory:", endOfCentralDirectory.totalEntryCount, "entries", endOfCentralDirectory.centralDirectorySize, "bytes @", endOfCentralDirectory.offset)
            zipFile?.seek(toFileOffset: UInt64(endOfCentralDirectory.offset))
        } else {
//            print("central directory:", endOfCentralDirectory.totalNumberOfEntriesInCentralDirectory, "entries", endOfCentralDirectory.centralDirectorySize, "bytes @", endOfCentralDirectory.centralDirectoryStartOffset)
            zipFile?.seek(toFileOffset: UInt64(endOfCentralDirectory.centralDirectoryStartOffset))
        }
        
        while true {
            let signature = try readUInt32()
            guard signature == CentralDirectoryHeader.signature else {
//                print("unexpected signature:", String(signature, radix: 16, uppercase: false))
                break
            }
            try enumerator?(Result {
                try CentralDirectoryHeader(reader: self, decompressor: decompressor)
            })
        }
    }
    
    private func findEndOfCentralDirectory() throws -> EndOfCentralDirectory {
        assert(zipFile != nil)
        guard let fileHandle = zipFile else {
            throw JebiZipError.invalidState
        }
        let fileSize = fileHandle.seekToEndOfFile()
        let minimumSizeOfEndOfCentralDirectory = UInt64(
            22
//            4 + 2 + 2 + 2 + 2 + 4 + 4 + 2
        ) // TODO: better way?
//        print("file size:", fileSize, "min. end of central directory size:", minimumSizeOfEndOfCentralDirectory)
        guard fileSize >= minimumSizeOfEndOfCentralDirectory else {
            throw JebiZipError.corruptFile
        }
        var position = fileSize - minimumSizeOfEndOfCentralDirectory
        
        while true {
            fileHandle.seek(toFileOffset: position)
            if try readUInt32() == EndOfCentralDirectory.signature {
//                print("end of central directory at:", position)
                return try EndOfCentralDirectory(reader: self)
            }
            guard position > 0 else {
                throw JebiZipError.corruptFile
            }
            position -= 1
        }
    }
    
    private func findZip64EndOfCentralDirectoryLocator(endOfCentralDirectory: EndOfCentralDirectory) throws -> Zip64EndOfCentralDirectoryLocator {
        let delta = UInt64(
            24
//            4 + 4 + 8 + 4 + 4
        )
        zipFile?.seek(toFileOffset: endOfCentralDirectory.offset - delta)
        guard try readUInt32() == Zip64EndOfCentralDirectoryLocator.signature else {
            // TODO: search?
            throw JebiZipError.notZip64
        }
        return try Zip64EndOfCentralDirectoryLocator(reader: self)
    }
    
    private func readLocalFileHeader() throws {
        try enumerator?(Result {
            try Entry(reader: self, decompressor: decompressor)
        })
    }
    
}

@available(iOS 9.0, macOS 10.11, *)
extension Zip: Reader {
    
    internal func readString(ofLength n: Int) throws -> String {
        guard let string = String(data: try readData(ofLength: n), encoding: .isoLatin1) else {
            throw JebiZipError.invalidStringEncoding
        }
        return string
    }
    
    internal func readUInt16() throws -> UInt16 {
        return try read()
    }
    
    internal func readUInt32() throws -> UInt32 {
        return try read()
    }
    
    internal func readUInt64() throws -> UInt64 {
        return try read()
    }
    
    private func read<T>() throws -> T {
        let size = MemoryLayout<T>.size
        return try readData(ofLength: size).withUnsafeBytes { $0.bindMemory(to: T.self)[0] }
    }
    
    internal func readData(ofLength n: Int) throws -> Data {
        guard let data = zipFile?.readData(ofLength: n), data.count == n else {
            throw JebiZipError.corruptFile
        }
        return data
    }

}

@available(iOS 9.0, macOS 10.11, *)
public func unzip(_ source: URL, to destination: URL) throws {
    try Zip(url: source).extract(to: destination)
}
