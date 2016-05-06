require 'log_tools/upgrade'

module Pocolog
    # Class that provides the functionality to "upgrade" older log files to
    # match the new type definitions
    class Upgrader
        # Build a {Upgrade::TypeConvertion} object suitable for a given
        # convertion
        def resolve_convertion(time, from_type, to_type)
        end
    end
end

