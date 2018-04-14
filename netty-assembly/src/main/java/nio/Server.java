package nio;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * @description: netty server
 * @author: kylin.qiuwx@foxmail.com
 * @create: 2018-04-14
 **/
public class Server {
    private void bind(int port) {

        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            //服务端辅助启动类，用以降低服务端的开发复杂度
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    //实例化ServerSocketChannel
                    .channel(NioServerSocketChannel.class)
                    //设置ServerSocketChannel的TCP参数
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .childHandler(new ChildChannelHandler());

            // ChannelFuture：代表异步I/O的结果
            ChannelFuture f = bootstrap.bind(port).sync();
            f.channel().closeFuture().sync();




        } catch (InterruptedException e) {
            System.out.println("启动netty服务异常");
            e.printStackTrace();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }

    }

    private class ChildChannelHandler extends ChannelInitializer<SocketChannel> {

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            //handler
            ch.pipeline().addLast(new ServerHandler());
        }

    }

    public static void main(String[] args) {
        int port = 8888;
        new Server().bind(port);

    }
}
