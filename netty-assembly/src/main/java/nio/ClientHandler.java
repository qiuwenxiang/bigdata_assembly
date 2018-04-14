package nio;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class ClientHandler extends ChannelInboundHandlerAdapter{

    private final ByteBuf byteBuf;

    public ClientHandler() {
        byte[] bytes = "{'id':1,'name':'zhangsan'}".getBytes();
        byteBuf = Unpooled.buffer(bytes.length);
        byteBuf.writeBytes(bytes);//写入buffer
    }


    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        //向服务端发送数据
        ctx.writeAndFlush(byteBuf);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        //读取服务端发过来的数据
        ByteBuf buf = (ByteBuf)msg;
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        String message = new String(bytes, "UTF-8");
        System.out.println("客户端收到的消息： " + message);
    }

}
