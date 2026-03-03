--!strict
local Players = game:GetService("Players")
local ReplicatedStorage = game:GetService("ReplicatedStorage")
local RunService = game:GetService("RunService")

export type Scope = string
export type Key = string
export type Value = any
export type ChangeCallback = (newValue: Value, oldValue: Value, key: Key, scope: Scope) -> ()
export type Middleware = (key: Key, scope: Scope, newValue: Value, oldValue: Value) -> (boolean, Value?)

local Signal = require(ReplicatedStorage.Packages.Signal)
local Network = require(script.Network)

-- ─── Internal store ──────────────────────────────────────────────────────────
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

-- ─── Module ──────────────────────────────────────────────────────────────────
--[=[
	@class ReplicatedData

	This is the ReplicatedData class.
]=]
local ReplicatedData = {}
ReplicatedData.__index = ReplicatedData
ReplicatedData.Signal = Signal.new()

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

function ReplicatedData._applyDelete(key: Key, scope: Scope)
	ensureScope(scope)
	local old = store[scope][key]
	store[scope][key] = nil
	fireListeners(key, scope, nil, old)

	ReplicatedData.Signal:Fire("delete", key, scope)
	return true
end

function ReplicatedData._applyBulk(scope: Scope, data: { [Key]: Value })
	store[scope] = data
	for k, v in data do
		fireListeners(k, scope, v, nil)
	end
end

-- ── Read ──────────────────────────────────────────────────────────────────────

--- Gets a value from the store.
--- @param key string -- The key to look up
--- @param scope string -- The scope to search in
--- @return any -- The stored value, or nil
function ReplicatedData:Get(key: Key, scope: Scope): Value
	return store[scope] and store[scope][key]
end

function ReplicatedData:WaitFor(key: Key, scope: Scope): Value
	local value = self:Get(key, scope)

	repeat
		value = self:Get(key, scope)
		task.wait(0.1)
	until value ~= nil

	return value
end

function ReplicatedData:GetScope(scope: Scope): { [Key]: Value }
	local copy = {}
	if store[scope] then
		for k, v in store[scope] do
			copy[k] = v
		end
	end
	return copy
end

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

function ReplicatedData:Has(key: Key, scope: Scope): boolean
	return store[scope] ~= nil and store[scope][key] ~= nil
end

function ReplicatedData:Keys(scope: Scope): { Key }
	local keys = {}
	if store[scope] then
		for k in store[scope] do
			table.insert(keys, k)
		end
	end
	return keys
end

function ReplicatedData:Scopes(): { Scope }
	local scopes = {}
	for s in store do
		table.insert(scopes, s)
	end
	return scopes
end

-- ── Write ─────────────────────────────────────────────────────────────────────

function ReplicatedData:Set(key: Key, scope: Scope, value: Value): boolean
	ensureScope(scope)
	local old = store[scope][key]
	local allowed, final = runMiddleware(key, scope, value, old)
	if not allowed then
		return false
	end
	return ReplicatedData._applySet(key, scope, final)
end

function ReplicatedData:Update(key: Key, scope: Scope, updater: (current: Value) -> Value): boolean
	return self:Set(key, scope, updater(self:Get(key, scope)))
end

function ReplicatedData:Increment(key: Key, scope: Scope, amount: number?): boolean
	local n = amount or 1
	return self:Update(key, scope, function(v)
		return (type(v) == "number" and v or 0) + n
	end)
end

function ReplicatedData:Delete(key: Key, scope: Scope)
	ReplicatedData._applyDelete(key, scope)
end

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

function ReplicatedData:BeginBatch()
	isBatching = true
end

function ReplicatedData:BatchSet(key: Key, scope: Scope, value: Value)
	if isBatching then
		table.insert(batchQueue, { scope = scope, key = key, value = value })
	else
		self:Set(key, scope, value)
	end
end

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

function ReplicatedData:RollbackBatch()
	isBatching = false
	batchQueue = {}
end

-- ── Middleware ────────────────────────────────────────────────────────────────

function ReplicatedData:UseMiddleware(handler: Middleware)
	table.insert(middlewareStack, handler)
end

function ReplicatedData:RemoveMiddleware(handler: Middleware)
	for i, mw in middlewareStack do
		if mw == handler then
			table.remove(middlewareStack, i)
			return
		end
	end
end

-- ── Subscriptions ─────────────────────────────────────────────────────────────

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

function ReplicatedData:Observe(key: Key, scope: Scope, callback: ChangeCallback): () -> ()
	task.spawn(callback, self:Get(key, scope), nil, key, scope)
	return self:OnChange(key, scope, callback)
end

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

-- ── Replication Filtering ─────────────────────────────────────────────────────────────
function ReplicatedData:SetScopeFilter(scope: Scope, fn: (player: Player, key: Key, value: Value) -> boolean)
	scopeFilters[scope] = fn
end

function ReplicatedData:RemoveScopeFilter(scope: Scope)
	scopeFilters[scope] = nil
end

-- ── Syncing ─────────────────────────────────────────────────────────────
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
                snapshot = filtered
            }, player)
        end)

		print("RD is syncing on the server.")
	end
end

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

-- ── Dev reset ─────────────────────────────────────────────────────────────────
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
