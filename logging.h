#pragma once

#include "allocator.h"
#include "bytesize.hh"
#include "filejob.h"

template <> struct fmt::formatter<FileCopyJob> : fmt::formatter<std::string> {
    auto format(const FileCopyJob &my, format_context &ctx) const
        -> decltype(ctx.out()) {
        return fmt::format_to(
            ctx.out(),
            "[FileCopyJob remote={}, partial={}, "
            "fileSize={}, blockSize={}, nScheduled={}, nFinished={}/{}]",
            !isNullRef(my.remote)
                ? stringrefImmediateToCString(globalAllocator, my.remote)
                : "NULL",
            !isNullRef(my.partial)
                ? stringrefImmediateToCString(globalAllocator, my.partial)
                : "NULL",
            my.stx.stx_size, my.blockSize, my.nBlockJobsScheduled,
            my.nBlockJobsFinished, blockCount(my.stx.stx_size, my.blockSize));
    }
};

template <> struct fmt::formatter<BlockCopyJob> : fmt::formatter<std::string> {
    auto format(const BlockCopyJob &my, format_context &ctx) const
        -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(),
                              "[BlockCopyJob parent={}, type={}, status={}, "
                              "offset={}, nbytes={}, buf={}]",
                              *my.parent, rwStrings[my.type], my.status,
                              my.offset, my.nbytes, (void *)my.data);
    }
};

// make the bytesize formatter known
BYTESIZE_FMTLIB_FORMATTER
