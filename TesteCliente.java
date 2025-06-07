import StableMulticast.*;

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

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Uso: java TesteCliente <ip_local> <porta_local>");
        }
        Scanner scanner = new Scanner(System.in);
        String ip = "127.0.0.1";
        int port = scanner.nextInt();
        TesteCliente cliente = new TesteCliente(ip, port);
        cliente.start();
    }
}
