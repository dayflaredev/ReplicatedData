--!strict
local Players = game:GetService("Players")
local ReplicatedStorage = game:GetService("ReplicatedStorage")
local RunService = game:GetService("RunService")

--[=[
	@type Scope string
	@within ReplicatedData
	A string identifier that groups related keys together.
	For example, `"player_123"` or `"global"`.
]=]
export type Scope = string

--[=[
	@type Key string
	@within ReplicatedData
	A string identifier for a value within a [Scope].
]=]
export type Key = string

--[=[
	@type Value any
	@within ReplicatedData
	Any value that can be stored and replicated.
]=]
export type Value = any

--[=[
	@type ChangeCallback (newValue: Value, oldValue: Value, key: Key, scope: Scope) -> ()
	@within ReplicatedData
	Callback fired when a value changes. Receives the new value, the old value,
	and the key/scope that changed.
]=]
export type ChangeCallback = (newValue: Value, oldValue: Value, key: Key, scope: Scope) -> ()

--[=[
	@type Middleware (key: Key, scope: Scope, newValue: Value, oldValue: Value) -> (boolean, Value?)
	@within ReplicatedData
	A function that intercepts writes before they are applied.

	Return `false` to block the write entirely.
	Return `true, transformedValue` to allow the write with an optionally transformed value.
	Return `true` (no second return) to allow the write unchanged.
]=]
export type Middleware = (key: Key, scope: Scope, newValue: Value, oldValue: Value) -> (boolean, Value?)

local Signal = require(ReplicatedStorage.Packages.Signal)
local Network = require(script.Network)

local store: { [Scope]: { [Key]: Value } } = {}
local keyListeners: { [string]: { ChangeCallback } } = {}
local scopeListeners: { [Scope]: { ChangeCallback } } = {}
local globalListeners: { ChangeCallback } = {}
local middlewareStack: { Middleware } = {}
local batchQueue: { { scope: Scope, key: Key, value: Value } } = {}
local isBatching = false
local scopeFilters: { [Scope]: (player: Player, key: Key, value: Value) -> boolean } = {}

local function makeId(scope: Scope, key: Key): string
	return scope .. "::" .. key
end

local function ensureScope(scope: Scope)
	if not store[scope] then
		store[scope] = {}
	end
end

local function fireListeners(key: Key, scope: Scope, newVal: Value, oldVal: Value)
	local lid = makeId(scope, key)
	if keyListeners[lid] then
		for _, cb in keyListeners[lid] do
			task.spawn(cb, newVal, oldVal, key, scope)
		end
	end
	if scopeListeners[scope] then
		for _, cb in scopeListeners[scope] do
			task.spawn(cb, newVal, oldVal, key, scope)
		end
	end
	for _, cb in globalListeners do
		task.spawn(cb, newVal, oldVal, key, scope)
	end
end

local function runMiddleware(key: Key, scope: Scope, newVal: Value, oldVal: Value): (boolean, Value)
	local final = newVal
	for _, mw in middlewareStack do
		local ok, transformed = mw(key, scope, final, oldVal)
		if not ok then
			return false, oldVal
		end
		if transformed ~= nil then
			final = transformed
		end
	end
	return true, final
end

--[=[
	@class ReplicatedData

	ReplicatedData is a server-authoritative key/value store that automatically
	replicates data from the server to clients.
]=]
local ReplicatedData = {}
ReplicatedData.__index = ReplicatedData

--[=[
	@prop Signal Signal
	@within ReplicatedData
	@private
	Internal signal used to bridge data changes to the network layer.
	Fires `"diff"` on a set and `"delete"` on a delete.
]=]
ReplicatedData.Signal = Signal.new()

-- ── Bridge ────────────────────────────────────────────────────────────────────

--[=[
	@private
	Writes a value directly to the store, bypassing middleware.
	Fires listeners and the internal Signal. Used by the networking layer.

	@param key Key
	@param scope Scope
	@param value Value
	@return boolean -- Whether the value actually changed
]=]
function ReplicatedData._applySet(key: Key, scope: Scope, value: Value): boolean
	ensureScope(scope)
	local old = store[scope][key]
	if old == value then
		return false
	end
	store[scope][key] = value
	fireListeners(key, scope, value, old)
	ReplicatedData.Signal:Fire("diff", key, scope, value, old)
	return true
end

--[=[
	@private
	Deletes a key directly from the store, bypassing middleware.
	Fires listeners and the internal Signal. Used by the networking layer.

	@param key Key
	@param scope Scope
]=]
function ReplicatedData._applyDelete(key: Key, scope: Scope)
	ensureScope(scope)
	local old = store[scope][key]
	store[scope][key] = nil
	fireListeners(key, scope, nil, old)
	ReplicatedData.Signal:Fire("delete", key, scope)
	return true
end

--[=[
	@private
	Replaces an entire scope's data in one operation, bypassing middleware.
	Used by the client during initial sync to apply the server snapshot.

	@param scope Scope
	@param data { [Key]: Value }
]=]
function ReplicatedData._applyBulk(scope: Scope, data: { [Key]: Value })
	store[scope] = data
	for k, v in data do
		fireListeners(k, scope, v, nil)
	end
end

-- ── Read ──────────────────────────────────────────────────────────────────────

--[=[
	Returns the value stored at `key` within `scope`, or `nil` if not set.

	@param key Key
	@param scope Scope
	@return Value
]=]
function ReplicatedData:Get(key: Key, scope: Scope): Value
	return store[scope] and store[scope][key]
end

--[=[
	Yields until `key` within `scope` is non-nil, then returns the value.
	Polls every 0.1 seconds.

	@yields
	@param key Key
	@param scope Scope
	@return Value
]=]
function ReplicatedData:WaitFor(key: Key, scope: Scope): Value
	local value = self:Get(key, scope)
	repeat
		value = self:Get(key, scope)
		task.wait(0.1)
	until value ~= nil
	return value
end

--[=[
	Returns a shallow copy of all key/value pairs in `scope`.
	Returns an empty table if the scope does not exist.

	@param scope Scope
	@return { [Key]: Value }
]=]
function ReplicatedData:GetScope(scope: Scope): { [Key]: Value }
	local copy = {}
	if store[scope] then
		for k, v in store[scope] do
			copy[k] = v
		end
	end
	return copy
end

--[=[
	Returns a shallow copy of the entire store across all scopes.
	@return { [Scope]: { [Key]: Value } }
]=]
function ReplicatedData:GetAll(): { [Scope]: { [Key]: Value } }
	local copy = {}
	for s, keys in store do
		copy[s] = {}
		for k, v in keys do
			copy[s][k] = v
		end
	end
	return copy
end

--[=[
	Returns `true` if `key` exists in `scope`, even when its value is `false` or `0`.

	@param key Key
	@param scope Scope
	@return boolean
]=]
function ReplicatedData:Has(key: Key, scope: Scope): boolean
	return store[scope] ~= nil and store[scope][key] ~= nil
end

--[=[
	Returns an array of all keys present in `scope`.

	@param scope Scope
	@return { Key }
]=]
function ReplicatedData:Keys(scope: Scope): { Key }
	local keys = {}
	if store[scope] then
		for k in store[scope] do
			table.insert(keys, k)
		end
	end
	return keys
end

--[=[
	Returns an array of all known scope names.

	@return { Scope }
]=]
function ReplicatedData:Scopes(): { Scope }
	local scopes = {}
	for s in store do
		table.insert(scopes, s)
	end
	return scopes
end

-- ── Write ─────────────────────────────────────────────────────────────────────

--[=[
	Sets `key` in `scope` to `value`. Runs the middleware stack before applying.
	Returns `true` if the value changed, `false` if it was blocked or unchanged.

	:::caution Server only
	Writes should only originate on the server. The server will automatically
	replicate the change to all eligible clients.
	:::

	@server
	@param key Key
	@param scope Scope
	@param value Value
	@return boolean
]=]
function ReplicatedData:Set(key: Key, scope: Scope, value: Value): boolean
	ensureScope(scope)
	local old = store[scope][key]
	local allowed, final = runMiddleware(key, scope, value, old)
	if not allowed then
		return false
	end
	return ReplicatedData._applySet(key, scope, final)
end

--[=[
	Applies `updater` to the current value of `key` in `scope` and sets the result.
	`updater` receives the current value (or `nil`) and should return the new value.

	@server
	@param key Key
	@param scope Scope
	@param updater (current: Value) -> Value
	@return boolean
]=]
function ReplicatedData:Update(key: Key, scope: Scope, updater: (current: Value) -> Value): boolean
	return self:Set(key, scope, updater(self:Get(key, scope)))
end

--[=[
	Increments a numeric key by `amount` (default `1`).
	If the key is not yet set, it starts from `0`.

	@server
	@param key Key
	@param scope Scope
	@param amount number? -- Defaults to 1
	@return boolean
]=]
function ReplicatedData:Increment(key: Key, scope: Scope, amount: number?): boolean
	local n = amount or 1
	return self:Update(key, scope, function(v)
		return (type(v) == "number" and v or 0) + n
	end)
end

--[=[
	Deletes `key` from `scope`. Fires listeners with `nil` as the new value.

	@server
	@param key Key
	@param scope Scope
]=]
function ReplicatedData:Delete(key: Key, scope: Scope)
	ReplicatedData._applyDelete(key, scope)
end

--[=[
	Deletes every key in `scope`, then removes the scope itself.

	@server
	@param scope Scope
]=]
function ReplicatedData:DeleteScope(scope: Scope)
	if not store[scope] then
		return
	end
	for _, k in self:Keys(scope) do
		ReplicatedData._applyDelete(k, scope)
	end
	store[scope] = nil
end

-- ── Batch ─────────────────────────────────────────────────────────────────────

--[=[
	Begins a batch operation. All subsequent [ReplicatedData.BatchSet] calls will be
	queued rather than applied immediately. Call [ReplicatedData.CommitBatch] to apply
	or [ReplicatedData.RollbackBatch] to discard.

	@server
]=]
function ReplicatedData:BeginBatch()
	isBatching = true
end

--[=[
	Queues a write when inside a batch (after [ReplicatedData.BeginBatch]).
	Falls through to [ReplicatedData.Set] immediately if no batch is active.

	@server
	@param key Key
	@param scope Scope
	@param value Value
]=]
function ReplicatedData:BatchSet(key: Key, scope: Scope, value: Value)
	if isBatching then
		table.insert(batchQueue, { scope = scope, key = key, value = value })
	else
		self:Set(key, scope, value)
	end
end

--[=[
	Applies all queued batch writes and ends the batch.
	Returns the number of keys that actually changed.

	@server
	@return number
]=]
function ReplicatedData:CommitBatch(): number
	isBatching = false
	local changed = 0
	for _, e in batchQueue do
		if self:Set(e.key, e.scope, e.value) then
			changed += 1
		end
	end
	batchQueue = {}
	return changed
end

--[=[
	Discards all queued batch writes and ends the batch without applying anything.

	@server
]=]
function ReplicatedData:RollbackBatch()
	isBatching = false
	batchQueue = {}
end

-- ── Middleware ────────────────────────────────────────────────────────────────

--[=[
	Registers a [Middleware] handler. Handlers are executed in insertion order
	on every [ReplicatedData.Set] and [ReplicatedData.Update] call.

	Middleware does **not** run on internal `_apply*` bridge calls.

	@param handler Middleware
]=]
function ReplicatedData:UseMiddleware(handler: Middleware)
	table.insert(middlewareStack, handler)
end

--[=[
	Removes a previously registered [Middleware] handler.

	@param handler Middleware
]=]
function ReplicatedData:RemoveMiddleware(handler: Middleware)
	for i, mw in middlewareStack do
		if mw == handler then
			table.remove(middlewareStack, i)
			return
		end
	end
end

-- ── Subscriptions ─────────────────────────────────────────────────────────────

--[=[
	Subscribes to changes on a specific `key` within `scope`.
	Returns a function that disconnects the listener when called.


	@param key Key
	@param scope Scope
	@param callback ChangeCallback
	@return () -> ()
]=]
function ReplicatedData:OnChange(key: Key, scope: Scope, callback: ChangeCallback): () -> ()
	local lid = makeId(scope, key)
	if not keyListeners[lid] then
		keyListeners[lid] = {}
	end
	table.insert(keyListeners[lid], callback)
	return function()
		for i, cb in keyListeners[lid] do
			if cb == callback then
				table.remove(keyListeners[lid], i)
				return
			end
		end
	end
end

--[=[
	Subscribes to changes on **any** key within `scope`.
	Returns a function that disconnects the listener when called.

	@param scope Scope
	@param callback ChangeCallback
	@return () -> ()
]=]
function ReplicatedData:OnScopeChange(scope: Scope, callback: ChangeCallback): () -> ()
	if not scopeListeners[scope] then
		scopeListeners[scope] = {}
	end
	table.insert(scopeListeners[scope], callback)
	return function()
		for i, cb in scopeListeners[scope] do
			if cb == callback then
				table.remove(scopeListeners[scope], i)
				return
			end
		end
	end
end

--[=[
	Subscribes to **every** change in the entire store, regardless of scope or key.
	Returns a function that disconnects the listener when called.

	@param callback ChangeCallback
	@return () -> ()
]=]
function ReplicatedData:OnAnyChange(callback: ChangeCallback): () -> ()
	table.insert(globalListeners, callback)
	return function()
		for i, cb in globalListeners do
			if cb == callback then
				table.remove(globalListeners, i)
				return
			end
		end
	end
end

--[=[
	Subscribes to `key` in `scope` and **immediately fires** `callback` with the
	current value. Useful for initialising UI without separate init logic.

	@param key Key
	@param scope Scope
	@param callback ChangeCallback
	@return () -> ()
]=]
function ReplicatedData:Observe(key: Key, scope: Scope, callback: ChangeCallback): () -> ()
	task.spawn(callback, self:Get(key, scope), nil, key, scope)
	return self:OnChange(key, scope, callback)
end

--[=[
	Subscribes to `key` in `scope` and **automatically disconnects** after the
	first change fires.

	@param key Key
	@param scope Scope
	@param callback ChangeCallback
	@return () -> () -- Disconnect function (if you need to cancel before it fires)
]=]
function ReplicatedData:Once(key: Key, scope: Scope, callback: ChangeCallback): () -> ()
	local disconnect: (() -> ())?
	disconnect = self:OnChange(key, scope, function(newVal, oldVal, k, s)
		if disconnect then
			disconnect()
		end
		callback(newVal, oldVal, k, s)
	end)
	return disconnect :: () -> ()
end

-- ── Replication Filtering ─────────────────────────────────────────────────────

--[=[
	Sets a filter function for `scope` that controls which players receive its data.

	The filter runs on every write and on the initial sync for joining players.
	Return `true` to allow replication to that player, `false` to hide it.

	@server
	@param scope Scope
	@param fn (player: Player, key: Key, value: Value) -> boolean
]=]
function ReplicatedData:SetScopeFilter(scope: Scope, fn: (player: Player, key: Key, value: Value) -> boolean)
	scopeFilters[scope] = fn
end

--[=[
	Removes the replication filter for `scope`, allowing all players to receive it.

	@server
	@param scope Scope
]=]
function ReplicatedData:RemoveScopeFilter(scope: Scope)
	scopeFilters[scope] = nil
end

-- ── Syncing ───────────────────────────────────────────────────────────────────

local function getEligiblePlayers(scope: Scope, key: Key, value: Value): { Player }
	local filter = scopeFilters[scope]
	if not filter then
		return Players:GetPlayers()
	end
	local eligible = {}
	for _, player in Players:GetPlayers() do
		if filter(player, key, value) then
			table.insert(eligible, player)
		end
	end
	return eligible
end

--[=[
	Initialises server-side replication. Must be called once from a `Script`.

	- Listens for store changes and pushes deltas to eligible clients.
	- Sends a filtered full snapshot to each player when they join.

	@server
]=]
function ReplicatedData:_ensureServerSync()
	if RunService:IsServer() then
		local function diff(key: string, scope: string, newValue: any, oldValue: any)
			return Network.packets.apply.sendToList({
				key = key,
				scope = scope,
				newData = newValue,
				oldData = oldValue,
			}, getEligiblePlayers(scope, key, newValue))
		end

		local function delete(key: string, scope: string)
			return Network.packets.delete.sendToList({
				key = key,
				scope = scope,
			}, getEligiblePlayers(scope, key, nil))
		end

		ReplicatedData.Signal:Connect(function(mode: string, ...)
			if mode == "diff" then
				return diff(...)
			elseif mode == "delete" then
				return delete(...)
			end
		end)

		Players.PlayerAdded:Connect(function(player)
			local fullSnapshot = ReplicatedData:GetAll()
			local filtered: { [Scope]: { [Key]: Value } } = {}

			for scope, keys in fullSnapshot do
				local filter = scopeFilters[scope]
				filtered[scope] = {}
				for key, value in keys do
					if not filter or filter(player, key, value) then
						filtered[scope][key] = value
					end
				end
			end

			Network.packets.sync.sendTo({
				snapshot = filtered,
			}, player)
		end)

		print("RD is syncing on the server.")
	end
end

--[=[
	Initialises client-side replication. Must be called once from a `LocalScript`.

	- Applies the full snapshot received on join.
	- Listens for incremental set/delete packets from the server.

	@client
]=]
function ReplicatedData:_ensureClientSync()
	if RunService:IsClient() then
		Network.packets.sync.listen(function(data: { snapshot: any })
			local snapshot = data.snapshot
			for scope, keys in snapshot do
				ReplicatedData._applyBulk(scope, keys)
			end
		end)

		Network.packets.apply.listen(function(data: { key: string, scope: string, newData: unknown, oldData: unknown })
			return ReplicatedData:Set(data.key, data.scope, data.newData)
		end)

		Network.packets.delete.listen(function(data: { key: string, scope: string })
			return ReplicatedData:Delete(data.key, data.scope)
		end)

		print("RD is syncing on the client.")
	end
end

--[=[
	@private
	Wipes all store data and resets all internal state.
	Intended for use in tests or hot-reload scenarios only.
]=]
function ReplicatedData:_reset()
	store = {}
	keyListeners = {}
	scopeListeners = {}
	globalListeners = {}
	middlewareStack = {}
	batchQueue = {}
	isBatching = false
end

return ReplicatedData
