package adc;

import ch.ethz.ssh2.ChannelCondition;
import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

/**
 * 远程调用shell脚本
 */

public class ShellRPC {

    private static Connection conn;
    /**
     * 远程机器IP
     */
    private static String ip;
    /**
     * 用户名
     */
    private static String userName;
    /**
     * 密码
     */
    private static String passWord;
    private static String charset = Charset.defaultCharset().toString();

    private static final int TIME_OUT = 1000 * 5 * 60;


    /**
     * 登录
     *
     * @return
     * @throws IOException
     */
    private static boolean getConnection(String userName, String passWord, String ip, int port) throws IOException {
        ShellRPC.ip = ip;
        ShellRPC.userName = userName;
        ShellRPC.passWord = passWord;

        conn = new Connection(ip, port);
        conn.connect();
        return conn.authenticateWithPassword(userName, passWord);
    }

    /**
     * 执行脚本
     *
     * @param cmds
     * @return
     * @throws Exception
     */
    public static int exec(String cmds) throws Exception {
        InputStream stdOut = null;
        InputStream stdErr = null;
        String outStr = "";
        String outErr = "";
        int ret = -1;
        try {
            if (getConnection("root", "Rfh9mZDxerOqdM4V", "192.168.11.32", 12816)) {
                // Open a new {@link Session} on this connection
                Session session = conn.openSession();
                // Execute a command on the remote machine.
                session.execCommand(cmds);

                stdOut = new StreamGobbler(session.getStdout());
                outStr = processStream(stdOut, charset);
                System.out.println("outStr" + outStr);
                stdErr = new StreamGobbler(session.getStderr());
                outErr = processStream(stdErr, charset);
                System.out.println("outErr" + outErr);
                session.waitForCondition(ChannelCondition.EXIT_STATUS, TIME_OUT);

                ret = session.getExitStatus();
            } else {
                throw new Exception("登录远程机器失败" + ip); // 自定义异常类 实现略
            }
        } finally {
            if (conn != null) {
                conn.close();
            }
            stdOut.close();
            stdErr.close();
        }
        return ret;
    }

    /**
     * @param in
     * @param charset
     * @return
     * @throws Exception
     */
    private static String processStream(InputStream in, String charset) throws Exception {
        byte[] buf = new byte[1024];
        StringBuilder sb = new StringBuilder();
        while (in.read(buf) != -1) {
            sb.append(new String(buf, charset));
        }
        return sb.toString();
    }

    public static void main(String[] args) throws Exception {

        exec("/opt/software/a.sh");
    }

}
