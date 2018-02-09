module FileUtils
    module IOCTL
        IOC_NRBITS   = 8
        IOC_TYPEBITS = 8
        IOC_SIZEBITS = 14
        IOC_DIRBITS  = 2

        IOC_NRSHIFT   = 0
        IOC_TYPESHIFT = IOC_NRSHIFT + IOC_NRBITS
        IOC_SIZESHIFT = IOC_TYPESHIFT + IOC_TYPEBITS
        IOC_DIRSHIFT  = IOC_SIZESHIFT + IOC_SIZEBITS

        IOC_NONE = 0
        IOC_WRITE = 1
        IOC_READ = 2

        def self.IOC(dir, type, nr, size)
            (dir << IOC_DIRSHIFT) +
                (type << IOC_TYPESHIFT) +
                (nr   << IOC_NRSHIFT) +
                (size << IOC_SIZESHIFT)
        end

        def self.IOW(type, nr, size)
            IOC(IOC_WRITE, type, nr, size)
        end

        def self.IOR(type, nr, size)
            IOC(IOC_READ, type, nr, size)
        end
    end

    BTRFS_IOC_CLONE = IOCTL.IOW(0x94, 9, 4)
    @may_have_reflink = true
    if RUBY_PLATFORM !~ /linux/
        @may_have_reflink = false
    end

    # Exception raised if reflinks are unsupported on the host platform or if
    # the underlying filesystem does not support them
    class ReflinkUnsupported < NotImplementedError
    end

    # Check if the host platform may have reflink support
    def self.may_have_reflink?
        @may_have_reflink
    end

    # Copies a file, using reflink the way cp --reflink would if possible
    #
    # @param [String] from_path the path to the file that will be copied
    # @param [String] to_path the path to the destination file
    # @return [Boolean] true if the copy has used reflink, false if it was a
    #   plain copy
    # @raise [ReflinkUnsupported] if the underlying platform does not have reflink
    #   (which you can also test with may_have_reflink?)
    def self.cp_reflink(from_path, to_path)
        if may_have_reflink?
            from_io = File.open(from_path, 'r')
            to_io   = File.open(to_path, 'w')
            begin
                to_io.ioctl(BTRFS_IOC_CLONE, from_io.fileno)
            rescue Errno::EXDEV
                raise ReflinkUnsupported, "cannot reflink while crossing filesystem boundaries (from #{from_path} to #{to_path})"
            rescue Errno::ENOTTY
                raise ReflinkUnsupported, "the backing filesystem of #{from_path} does not support reflinks"
            rescue Errno::EOPNOTSUPP
                raise ReflinkUnsupported, "reflinks are not supported on this platform"
            end
        else
            raise ReflinkUnsupported, "reflinks are not supported on this platform"
        end
    end

    # Attempts to copy using CoW if possible
    #
    # @param [Array<Symbol>] strategies to try. It is a list of :reflink,
    #   :hardlink, :cp. The default is :reflink and :cp, :hardlink being usable
    #   only if the source and target files are meant to be read-only
    def self.cp_cow(from_path, to_path, strategies: [:reflink, :cp])
        strategies.each do |s|
            case s
            when :reflink
                begin
                    cp_reflink(from_path, to_path)
                    return :reflink
                rescue ReflinkUnsupported
                end
            when :hardlink
                begin
                    FileUtils.ln from_path, to_path
                    return :hardlink
                rescue Errno::EPERM
                    # This is what we get on filesystems that do not support
                    # hardlinks
                end
            when :cp
                # We assume that 'cp' should always work
                FileUtils.cp from_path, to_path
                return :cp
            else
                raise ArgumentError, "unknown strategy for cp_cow #{s}, expected one of :reflink, :hardlink or :cp"
            end
        end
    end
end

