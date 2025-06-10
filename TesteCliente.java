import StableMulticast.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Scanner;

public class TesteCliente implements IStableMulticast {
    private StableMulticast sm;

    public TesteCliente(String ip, int port) {
        sm = new StableMulticast(ip, port, this);
    }

    @Override
    public void deliver(String msg) {
        System.out.println("[DELIVER] Mensagem recebida: " + msg);
    }

    public void start() {
        Scanner sc = new Scanner(System.in);
        while (true) {
            System.out.println("Digite uma mensagem para multicast (ou 'sair'): ");
            String msg = sc.nextLine();
            if (msg.equalsIgnoreCase("sair")) break;
            sm.msend(msg, this);
        }
        System.out.println("Encerrando cliente...");
        sc.close();
    }

    public static void main(String[] args) throws UnknownHostException {
        if (args.length < 1) {
            System.out.println("Uso: java TesteCliente <ip_local> <porta_local>");
            return;
        }
        String ip = InetAddress.getLocalHost().getHostAddress();
        int port = Integer.parseInt(args[0]);
        TesteCliente cliente = new TesteCliente(ip, port);
        cliente.start();
    }
}
