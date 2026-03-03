local ReplicatedStorage = game:GetService("ReplicatedStorage")
local ByteNet = require(ReplicatedStorage.Packages.ByteNet)

return ByteNet.defineNamespace("replicated_data", function()
	return {
		packets = {
			apply = ByteNet.definePacket({
				value = ByteNet.struct({
					key = ByteNet.string,
					scope = ByteNet.string,
					newData = ByteNet.unknown,
					oldData = ByteNet.unknown,
				}),
			}),
			delete = ByteNet.definePacket({
				value = ByteNet.struct({
					key = ByteNet.string,
					scope = ByteNet.string,
				}),
			}),
			sync = ByteNet.definePacket({
				value = ByteNet.struct({
					snapshot = ByteNet.unknown,
				}),
			}),
		}
	}
end)
