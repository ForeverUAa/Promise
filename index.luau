local Promise = {}
Promise.__index = Promise
Promise.__tostring = function(self) return string.format("Promise <%s>: %s", self.State, tostring(self.__address):sub(8)) end

export type Promise = {
	State: "pending" | "fulfilled" | "rejected" | "cancelled" | "unknown",

	resolve: (
		self: Promise,
		...any?
	) -> (),
	reject: (
		self: Promise,
		reason: string,
		...any?
	) -> (),

	next: (
		self: Promise,
		onFulfilled: (...any) -> ...any,
		onRejected: (reason: string, ...any) -> ...any
	) -> Promise,

	await: (
		self: Promise,
		suppressError: boolean?
	) -> (boolean, ...any) | any,
	cancel: (self: Promise) -> (),
}

function Promise.isPromise(p: Promise?): boolean
	return type(p) == "table" and getmetatable(p) == Promise
end

function Promise.new(executor)
	local self = {
		State = "pending",
		_handlers = {},
		_value = {},
	}
	self.__address = tostring(self)
	setmetatable(self, Promise)
	if executor then
		task.defer(function()
			local ok, response = pcall(
				executor,
				function(...) self:resolve(...) end,
				function(...) self:reject(...) end)
			if not ok then
				self:reject(response)
			end
		end)
	end
	return self
end

function Promise:_getValues(): ...any return table.unpack(self._value) end

function Promise:_executeHandler(handler): ()
	if self.State == "cancelled" then
		handler.child:cancel()
		return
	elseif self.State == "unknown" then
		handler.child:reject("Promise is in an unknown state", self:_getValues())
		return
	end

	local callback = handler[self.State]
	if callback then
		local result = table.pack(pcall(callback, table.unpack(self._value)))
		local ok = result[1]
		if ok then
			local response = result[2]
			if Promise.isPromise(response) then
				response:next(
		local result = table.pack(pcall(callback, self:_getValues()))
		local ok = result[1]
		if ok then
			local response = result[2]
			if Promise.isPromise(response) then 	
				response:next( 	
					function(...) handler.child:resolve(...) end,
					function(...) handler.child:reject(...) end)
			else
				handler.child:resolve(table.unpack(result, 2, result.n))
			end
		else
			handler.child:reject(table.unpack(result, 2, result.n))
		end
				handler.child:resolve(unpack(result, 2, result.n))
			end 		 
		else    
			handler.child:reject(unpack(result, 2, result.n))
		end 
	else
		if self.State == "fulfilled" then
			handler.child:resolve(self:_getValues())
		else
			handler.child:reject(self:_getValues())
		end
	end
end

function Promise:resolve(...: any?): ()
	if self.State == "pending" then
		self.State = "fulfilled"
		self._value = {...}
		for _, h in ipairs({table.unpack(self._handlers)}) do self:_executeHandler(h) end
	end
end

function Promise:reject(...: any?): ()
	if self.State == "pending" then
		self.State = "rejected"
		self._value = {...}
		for _, h in ipairs({table.unpack(self._handlers)}) do self:_executeHandler(h) end
	end
end

function Promise:cancel(): ()
	if self.State == "pending" then
		for _, h in ipairs({table.unpack(self._handlers)}) do h.child:cancel() end
		self.State = "cancelled"
	end
end

function Promise:await(suppress)
	local handle = function()
		if self.State == "fulfilled" then
			if suppress then
				return true, self:_getValues()
			else
				return self:_getValues()
			end
		elseif self.State == "rejected" then
			if suppress then
				return false, self:_getValues()
			else
				local reason = self._value[1] and (' "' .. tostring(self._value[1]) .. '"') or ''
				return error("Promise yielded error" .. reason)
			end
		end
	end
	if self.State == "pending" then
		local thread = coroutine.running()
		local resume = function(...) coroutine.resume(thread, ...) end
		self:next(
			resume,
			resume)
		coroutine.yield()
		return handle()
	else
		return handle()
	end
end

function Promise:next(onFulfilled, onRejected): ()
	local new = Promise.new()
	local handler = {
		child = new,
		fulfilled = onFulfilled,
		rejected = onRejected,
	}

	if self.State == "pending" then
		table.insert(self._handlers, handler)
	elseif self.State == "cancelled" then
		new:cancel()
	elseif self.State == "unknown" then
		new:reject("Promise is in an unknown state", self:_getValues())
	else
		task.defer(self._executeHandler, self, handler)
	end

	return new
end

return (Promise :: {
	new: (
		executor: (
			resolve: (...any?) -> (),
			reject: (reason: string, ...any?
			) -> ()
		) -> ()) -> Promise,
	isPromise: (Promise?) -> boolean,
})
