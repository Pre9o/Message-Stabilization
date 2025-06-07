package StableMulticast;

import java.net.*;
import java.sql.SQLOutput;
import java.util.*;
import java.util.concurrent.*;
import java.io.*;

public class StableMulticast {
    private final String multicastIp = "230.0.0.1";
    private final int multicastPort = 4446;
    private final int bufferSize = 1024;
    private final int discoveryInterval = 2000;

    private final String localIp;
    private final int localPort;
    private final IStableMulticast client;

    private final Set<Member> group = ConcurrentHashMap.newKeySet();
    private final Map<String, int[]> vectorClocks = new ConcurrentHashMap<>();
    private final List<Message> buffer = Collections.synchronizedList(new ArrayList<>());

    private int[][] localMatrix;
    private int memberIndex;
    private volatile boolean running = true;

    private final Scanner scanner = new Scanner(System.in);

    public StableMulticast(String ip, Integer port, IStableMulticast client) {
        this.localIp = ip;
        this.localPort = port;
        this.client = client;
        group.add(new Member(localIp, localPort));
        // Descoberta de membros
        new Thread(this::discoveryService).start();
        // Recepção de mensagens
        new Thread(this::receiveService).start();
    }

    public void msend(String msg, IStableMulticast client) {
        int[] myRow = Arrays.copyOf(localMatrix[memberIndex], localMatrix.length);
        Message m = new Message(localIp, localPort, myRow, msg);

        buffer.add(m);

        printState();

        List<Member> memberList = new ArrayList<>(group);
        if (memberList.isEmpty()) {
            System.out.println("Nenhum membro para enviar.");
            return;
        }
        System.out.println("Enviar mensagem para TODOS os membros? (s/n)");
        String opt = scanner.nextLine().trim().toLowerCase();
        if (opt.equals("s")) {
            for (Member member : memberList) {
                sendUnicastWithPrompt(member, m, false);
            }
        } else {
            Set<Member> enviados = new HashSet<>();
            while (enviados.size() < memberList.size()) {
                System.out.println("Membros disponíveis:");
                for (int i = 0; i < memberList.size(); i++) {
                    Member mem = memberList.get(i);
                    if (!enviados.contains(mem)) {
                        System.out.println(i + ": " + mem);
                    }
                }
                System.out.print("Digite o número do membro para enviar (ou 'fim' para terminar): ");
                String entrada = scanner.nextLine().trim();
                if (entrada.equalsIgnoreCase("fim")) break;
                try {
                    int idx = Integer.parseInt(entrada);
                    if (idx >= 0 && idx < memberList.size() && !enviados.contains(memberList.get(idx))) {
                        sendUnicastWithPrompt(memberList.get(idx), m, true);
                        enviados.add(memberList.get(idx));
                    } else {
                        System.out.println("Índice inválido ou já enviado.");
                    }
                } catch (NumberFormatException e) {
                    System.out.println("Entrada inválida.");
                }
            }
        }

        localMatrix[memberIndex][memberIndex]++;
    }

    // Envio unicast com exibição de confirmação
    private void sendUnicastWithPrompt(Member member, Message m, boolean prompt) {
        if (prompt) {
            System.out.print("Enviar para " + member + "? (s/n): ");
            String resp = scanner.nextLine().trim().toLowerCase();
            if (!resp.equals("s")) {
                System.out.println("Envio para " + member + " adiado.");
                return;
            }
        }
        sendUnicast(member, m);
        System.out.println("Mensagem enviada para " + member);
    }

    private void discoveryService() {
        try (MulticastSocket socket = new MulticastSocket(multicastPort)) {
            InetAddress groupAddr = InetAddress.getByName(multicastIp);
            socket.joinGroup(groupAddr);

            // Envia presença periodicamente
            new Thread(() -> {
                while (running) {
                    try {
                        String announce = localIp + ":" + localPort;
                        DatagramPacket packet = new DatagramPacket(
                            announce.getBytes(), announce.length(), groupAddr, multicastPort);
                        socket.send(packet);
                        Thread.sleep(discoveryInterval);
                    } catch (Exception e) { /* ignore */ }
                }
            }).start();

            // Recebe presença de outros membros
            while (running) {
                byte[] buf = new byte[bufferSize];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);
                String data = new String(packet.getData(), 0, packet.getLength());
                String[] parts = data.split(":");
                if (parts.length == 2) {
                    String ip = parts[0];
                    int port = Integer.parseInt(parts[1]);
                    Member m = new Member(ip, port);
                    group.add(m);
                    updateMemberIndex();
                }
            }
        } catch (Exception e) { /* ignore */ }
    }

    private void updateMemberIndex() {
        List<Member> sorted = new ArrayList<>(group);
        sorted.sort(Comparator.comparing(Member::toString));
        int newIndex = -1;
        for (int i = 0; i < sorted.size(); i++) {
            if (sorted.get(i).equals(new Member(localIp, localPort))) {
                newIndex = i;
                break;
            }
        }
        memberIndex = newIndex;
        int n = sorted.size();
        boolean matrizNova = (localMatrix == null);
        if (matrizNova || localMatrix.length != n) {
            int[][] newMatrix = new int[n][n];
            for (int i = 0; i < n; i++)
                Arrays.fill(newMatrix[i], -1);
            if (!matrizNova) {
                for (int i = 0; i < Math.min(localMatrix.length, n); i++)
                    System.arraycopy(localMatrix[i], 0, newMatrix[i], 0, Math.min(localMatrix[i].length, n));
            }
            localMatrix = newMatrix;
        }

        if (localMatrix[memberIndex][memberIndex] == -1) {
            System.out.println("Inicializando relógio lógico para o processo local " + memberIndex);
            localMatrix[memberIndex][memberIndex] = 0;
            if (memberIndex > 0) {
                for (int i = memberIndex - 1; i >= 0; i--) {
                    localMatrix[i][i] = -1;
                }
            }
        }
    }

    private void receiveService() {
        try (DatagramSocket socket = new DatagramSocket(localPort)) {
            while (running) {
                byte[] buf = new byte[bufferSize];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);
                Message m = Message.fromBytes(packet.getData(), packet.getLength());
                if (m != null) {
                    buffer.add(m);
                    int senderIdx = getMemberIndex(m.senderIp, m.senderPort);
                    if (senderIdx != -1 && m.vector != null && m.vector.length == localMatrix.length) {
                        localMatrix[senderIdx] = Arrays.copyOf(m.vector, m.vector.length);
                    }
                    tryDeliver();
                }
            }
        } catch (Exception e) { /* ignore */ }
    }

    private void tryDeliver() {
        synchronized (buffer) {
            Iterator<Message> it = buffer.iterator();
            while (it.hasNext()) {
                Message m = it.next();
                if (isDeliverable(m)) {
                    client.deliver(m.msg);
                    updateVector(m);
                    it.remove();
                    printState();
                }
            }
        }
    }

    private void printState() {
        System.out.println("=== ESTADO ATUAL ===");
        System.out.println("Matriz de relógios lógicos:");
        for (int i = 0; i < localMatrix.length; i++) {
            System.out.print("P" + i + ": ");
            System.out.println(Arrays.toString(localMatrix[i]));
        }
        System.out.println("Buffer de mensagens:");
        synchronized (buffer) {
            for (Message m : buffer) {
                System.out.println("De " + m.senderIp + ":" + m.senderPort + " | Linha=" + Arrays.toString(m.vector) + " | Msg: " + m.msg);
            }
        }
        System.out.println("====================");
    }

    private boolean isDeliverable(Message m) {
        int senderIdx = getMemberIndex(m.senderIp, m.senderPort);
        if (senderIdx == -1) return false;
        for (int k = 0; k < localMatrix.length; k++) {
            if (k == senderIdx) continue;
            if (localMatrix[memberIndex][k] < m.vector[k]) {
                return false;
            }
        }
        if (localMatrix[memberIndex][senderIdx] != m.vector[senderIdx] - 1) {
            return false;
        }
    return true;
}

    private void updateVector(Message m) {
        int senderIdx = getMemberIndex(m.senderIp, m.senderPort);
        if (senderIdx == -1) return;
        localMatrix[memberIndex][senderIdx] = m.vector[senderIdx];
}

    private void sendUnicast(Member member, Message m) {
        try (DatagramSocket socket = new DatagramSocket()) {
            byte[] data = m.toBytes();
            DatagramPacket packet = new DatagramPacket(
                data, data.length, InetAddress.getByName(member.ip), member.port);
            socket.send(packet);
        } catch (Exception e) { /* ignore */ }
    }

    private int getMemberIndex(String ip, int port) {
        List<Member> sorted = new ArrayList<>(group);
        sorted.sort(Comparator.comparing(Member::toString));
        for (int i = 0; i < sorted.size(); i++) {
            if (sorted.get(i).equals(new Member(ip, port))) return i;
        }
        return -1;
    }

    private static class Member {
        String ip;
        int port;
        Member(String ip, int port) { this.ip = ip; this.port = port; }
        public boolean equals(Object o) {
            if (!(o instanceof Member)) return false;
            Member m = (Member) o;
            return ip.equals(m.ip) && port == m.port;
        }
        public int hashCode() { return Objects.hash(ip, port); }
        public String toString() { return ip + ":" + port; }
    }

    private static class Message {
        String senderIp;
        int senderPort;
        int[] vector; 
        String msg;

        Message(String ip, int port, int[] vector, String msg) {
            this.senderIp = ip;
            this.senderPort = port;
            this.vector = vector;
            this.msg = msg;
        }

        byte[] toBytes() {
            // Serializa: ip:port|v0,v1,v2,...|msg
            StringBuilder sb = new StringBuilder();
            sb.append(senderIp).append(":").append(senderPort).append("|");
            for (int i = 0; i < vector.length; i++) {
                sb.append(vector[i]);
                if (i < vector.length - 1) sb.append(",");
            }
            sb.append("|").append(msg);
            return sb.toString().getBytes();
        }

        static Message fromBytes(byte[] data, int len) {
            try {
                String s = new String(data, 0, len);
                String[] parts = s.split("\\|");
                String[] ipPort = parts[0].split(":");
                String[] vparts = parts[1].split(",");
                int[] vector = new int[vparts.length];
                for (int i = 0; i < vparts.length; i++) vector[i] = Integer.parseInt(vparts[i]);
                return new Message(ipPort[0], Integer.parseInt(ipPort[1]), vector, parts[2]);
            } catch (Exception e) { return null; }
        }
    }
}