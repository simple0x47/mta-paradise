--[[
Copyright (c) 2021 MTA: Paradise Extended

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
]]

local connection = nil
local query_handles = { }
local max_query_handles = 128
local poll_timeout = get( "poll_timeout" ) or 500

local function createHostString( server, db, port, socket )
	local host_string = "dbname=" .. db .. ";host=" .. server .. ";port=" .. 3306

	if (socket ~= nil) then
		host_string = host_string .. ";unix_socket=" .. socket
	end

	return host_string
end

-- connection functions
local function connect( )
	-- retrieve the settings
	local server = get( "server" ) or "localhost"
	local user = get( "user" ) or "root"
	local password = get( "password" ) or ""
	local db = get( "database" ) or "mta"
	local port = get( "port" ) or 3306
	local socket = get( "socket" ) or ""
	
	-- connect
	local host_string = createHostString( server, db, port, socket )
	connection = Connection( "mysql", host_string, user, password )

	if connection then
		if user == "root" then
			setTimer( outputDebugString, 100, 1, "Connecting to your MySQL as 'root' is strongly discouraged.", 2 )
		end

		return true
	else
		outputDebugString ( "Connection to MySQL Failed.", 1 )
		return false
	end
end

local function disconnect( )
	if connection then
		connection:destroy( )

		connection = nil
	end
end

local function reconnectIfDisconnected( )
	if not connection then
		return connect( )
	end

	return true
end

addEventHandler( "onResourceStart", resourceRoot,
	function( )
		if not hasObjectPermissionTo( resource, "function.mysql_connect" ) then
			if hasObjectPermissionTo( resource, "function.shutdown" ) then
				shutdown( "Insufficient ACL rights for mysql resource." )
			end
			cancelEvent( true, "Insufficient ACL rights for mysql resource." )
		elseif not connect( ) then
			if hasObjectPermissionTo( resource, "function.shutdown" ) then
				shutdown( "MySQL failed to connect." )
			end
			cancelEvent( true, "MySQL failed to connect." )
		end
	end
)

addEventHandler( "onResourceStop", resourceRoot,
	function( )
		for key, value in pairs( query_handles ) do
			value.instance:free( )
			outputDebugString( "Query freed at stop: " .. value.q, 2 )
		end
		
		disconnect( )
	end
)

--

function escape_string( str )
	if type( str ) == "string" then
		return connection:prepareString( str )
	elseif type( str ) == "number" then
		return tostring( str )
	end
end

local function joinArguments( str, ... )
	if ( ... ) then
		local t = { ... }
		for k, v in ipairs( t ) do
			t[ k ] = escape_string( tostring( v ) ) or ""
		end
		str = str:format( unpack( t ) )
	end

	return str
end

local function query( str, ... )
	reconnectIfDisconnected( )
	
	str = joinArguments( str, ... )
	
	local query_handle = connection:query( str )
	if query_handle then
		for num = 1, max_query_handles do
			if not query_handles[ num ] then
				query_handles[ num ] = { instance = query_handle, q = str }
				return num
			end
		end
		query_handle:free( )
		return false, "Unable to allocate query handle in pool"
	end

	return false, "" -- Empty string used as mysql error string for compatibility reasons
end

local function isSourceResourceNotAllowed()
	return sourceResource == getResourceFromName( "runcode" )
end

function query_free( str, ... )
	if isSourceResourceNotAllowed() then
		return false
	end
	
	reconnectIfDisconnected( )
	
	str = joinArguments( str, ... )
	
	local query_handle = connection:query( str )
	if query_handle then
		query_handle:free( )
		return true
	end

	return false, "" -- Empty string used as mysql error string for compatibility reasons
end

function free_query_handle( query_handle_index )
	if query_handles[ query_handle_index ] then
		query_handles[ query_handle_index ].instance:free( )
		query_handles[ query_handle_index ] = nil
	end
end

-- To be used when the query handle has already been freed.
function remove_query_handle( query_handle_index )
	if query_handles[ query_handle_index ] then
		query_handles[ query_handle_index ] = nil
	end
end

local function pollQueryHandleByIndex( query_handle_index, multiple_result )
	local query_handle = query_handles[ query_handle_index ].instance
	local result, error_code_or_affected_rows, error_or_id = query_handle:poll( poll_timeout, multiple_result )

	return result, error_code_or_affected_rows, error_or_id
end

function query_assoc( str, ... )
	if isSourceResourceNotAllowed() then
		return false
	end
	
	local query_handle_index, error = query( str, ... )
	if query_handle_index then
		local result, error_code, error = pollQueryHandleByIndex( query_handle_index, false )

		if result then
			local t = { }

			for i = 1, #result do 
				t[ i ] = { }

				local row = result[ i ]
				for key, value in pairs( row ) do
					if value ~= nil then
						t[ i ][ key ] = tonumber( value ) or value
					end
				end
			end

			remove_query_handle( query_handle_index )
			return t
		elseif ( result == nil ) then
			free_query_handle( query_handle_index )
			error = "Poll timeout."
		end
	end

	return false, error
end

function query_assoc_single( str, ... )
	if isSourceResourceNotAllowed() then
		return false
	end
	
	local query_handle_index, error = query( str, ... )
	if query_handle_index then
		local result, error_code, error = pollQueryHandleByIndex( query_handle_index, false )

		if result then
			--[[
				We select the first element of the 'result' table as to simulate mysql_fetch_assoc().
				Because this function creates a query each time it is called, there's no need to 
					implement mysql_fetch_assoc's additional behaviour.
			]]
			local row = result[ 1 ]

			remove_query_handle( query_handle_index )
			return row
		elseif ( result == nil ) then
			free_query_handle( query_handle_index )
			error = "Poll timeout."
		end

		remove_query_handle( query_handle_index )
		return false, error
	end

	return false, error
end

function query_insertid( str, ... )
	if isSourceResourceNotAllowed() then
		return false
	end
	
	local query_handle_index, error = query( str, ... )
	
	if query_handle_index then
		local result, affected_rows, error_or_id = pollQueryHandleByIndex( query_handle_index, false )

		if result then
			remove_query_handle( query_handle_index )
			return error_or_id
		elseif ( result == nil ) then
			free_query_handle( query_handle_index )
			error = "Poll timeout."
		else
			error = error_or_id
		end

		remove_query_handle( query_handle_index )
	end

	return false, error
end

function query_affected_rows( str, ... )
	if isSourceResourceNotAllowed() then
		return false
	end
	
	local query_handle_index, error = query( str, ... )
	
	if query_handle_index then
		local result, affected_rows, error_or_id = pollQueryHandleByIndex( query_handle_index, false )

		if result then
			remove_query_handle( query_handle_index )
			return affected_rows
		elseif ( result == nil ) then
			free_query_handle( query_handle_index )
			error = "Poll timeout."
		else
			error = error_or_id
		end

		remove_query_handle( query_handle_index )
	end

	return false, error
end