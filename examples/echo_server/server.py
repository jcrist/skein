import asyncio
import socket
import skein


async def handle_echo(reader, writer):
    """Generic python tcp echo server"""
    data = await reader.read(100)
    message = data.decode()
    print("Received %r" % message)

    writer.write(data)
    await writer.drain()
    print("Sent: %r" % message)

    writer.close()


# Setup the server with a dynamically chosen port
loop = asyncio.get_event_loop()
coro = asyncio.start_server(handle_echo, '0.0.0.0', 0, loop=loop)
server = loop.run_until_complete(coro)

# Determine the dynamically chosen address
host = socket.gethostbyname(socket.gethostname())
port = server.sockets[0].getsockname()[1]
address = '%s:%d' % (host, port)

# Form a unique key to store the address using the current container id
key = 'address.%s' % skein.properties.container_id

# Connect to the application master
app = skein.ApplicationClient.from_current()

# The key-value store only accepts bytes as values
value = address.encode()

# Store the server address in the key-value store, assigning the current
# container as the owner of the key. This ensures that the key is deleted if
# the container exits.
app.kv.put(key, value, owner=skein.properties.container_id)

# Serve requests until shutdown
loop.run_forever()

# Cleanup
server.close()
loop.run_until_complete(server.wait_closed())
loop.close()
