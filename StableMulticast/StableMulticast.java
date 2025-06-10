package StableMulticast;

import java.net.*;
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

    private final Map<Member, Long> lastSeen = new ConcurrentHashMap<>();
    private final long memberTimeout = 6000; 

    public StableMulticast(String ip, Integer port, IStableMulticast client) {
        this.localIp = ip;
        this.localPort = port;
        this.client = client;
        group.add(new Member(localIp, localPort));
        lastSeen.put(new Member(localIp, localPort), System.currentTimeMillis());
        // Descoberta de membros
        new Thread(this::discoveryService).start();
        // Recepção de mensagens
        new Thread(this::receiveService).start();
        // Thread para monitorar membros ausentes
        new Thread(this::memberTimeoutService).start();
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
        printState();
    }

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
                    lastSeen.put(m, System.currentTimeMillis()); // Atualiza último anúncio
                    updateMemberIndex();
                }
            }
        } catch (Exception e) { /* ignore */ }
    }

    private void memberTimeoutService() {
        while (running) {
            try {
                long now = System.currentTimeMillis();
                List<Member> toRemove = new ArrayList<>();
                for (Member m : group) {
                    if (m.equals(new Member(localIp, localPort))) continue;
                    Long seen = lastSeen.get(m);
                    if (seen == null || now - seen > memberTimeout) {
                        toRemove.add(m);
                    }
                }
                if (!toRemove.isEmpty()) {
                    for (Member m : toRemove) {
                        System.out.println("Removendo membro ausente: " + m);
                        group.remove(m);
                        lastSeen.remove(m);
                        removeMemberFromMatrix(m);
                        removeMessagesFromBuffer(m);
                    }
                    updateMemberIndex();
                    printState();
                }
                Thread.sleep(1000);
            } catch (Exception e) { /* ignore */ }
        }
    }

    private void removeMemberFromMatrix(Member m) {
        List<Member> sorted = new ArrayList<>(group);
        sorted.sort(Comparator.comparing(Member::toString));
        int idx = -1;
        for (int i = 0; i < sorted.size() + 1; i++) { 
            if (i < sorted.size() && sorted.get(i).equals(m)) {
                idx = i;
                break;
            }
        }
        if (idx == -1 && localMatrix != null && localMatrix.length > 1) {
            List<Member> oldSorted = new ArrayList<>(group);
            oldSorted.add(m);
            oldSorted.sort(Comparator.comparing(Member::toString));
            for (int i = 0; i < oldSorted.size(); i++) {
                if (oldSorted.get(i).equals(m)) {
                    idx = i;
                    break;
                }
            }
        }
        if (idx != -1 && localMatrix != null && localMatrix.length > 1) {
            int n = localMatrix.length;
            int[][] newMatrix = new int[n - 1][n - 1];
            for (int i = 0, ni = 0; i < n; i++) {
                if (i == idx) continue;
                for (int j = 0, nj = 0; j < n; j++) {
                    if (j == idx) continue;
                    newMatrix[ni][nj] = localMatrix[i][j];
                    nj++;
                }
                ni++;
            }
            localMatrix = newMatrix;
        }
    }

    private void removeMessagesFromBuffer(Member m) {
        synchronized (buffer) {
            buffer.removeIf(msg -> msg.senderIp.equals(m.ip) && msg.senderPort == m.port);
        }
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
                
                try (ByteArrayInputStream byteStream = new ByteArrayInputStream(packet.getData(), 0, packet.getLength());
                     ObjectInputStream in = new ObjectInputStream(byteStream)) {
                    
                    Message m = (Message) in.readObject();
                    if (m != null) {
                        int senderIdx = getMemberIndex(m.senderIp, m.senderPort);
                        if (senderIdx == memberIndex) {
                            continue;
                        }
                        buffer.add(m);
                        if (senderIdx != -1 && m.vector != null && m.vector.length == localMatrix.length) {
                            // MCi[j][*] <- msg.VC (atualiza visão do Pi com visão de Pj)
                            for (int k = 0; k < localMatrix.length; k++) {
                                localMatrix[senderIdx][k] = m.vector[k];
                            }
                            // if i != j then MCi[i][j] <- MCi[i][j]+1
                            if (memberIndex != senderIdx) {
                                localMatrix[memberIndex][senderIdx]++;
                            }
                        }
                        client.deliver(m.msg);
                        discardDeliveredMessages();
                        printState();
                    }
                } catch (IOException | ClassNotFoundException e) {
                    System.err.println("Error deserializing message: " + e.getMessage());
                }
            }
        } catch (Exception e) { /* ignore */ }
    }

    private void discardDeliveredMessages() {
        synchronized (buffer) {
            Iterator<Message> it = buffer.iterator();
            while (it.hasNext()) {
                Message m = it.next();
                int senderIdx = getMemberIndex(m.senderIp, m.senderPort);
                if (senderIdx == -1) continue;
                int min = Integer.MAX_VALUE;
                for (int x = 0; x < localMatrix.length; x++) {
                    min = Math.min(min, localMatrix[x][senderIdx]);
                }
                if (m.vector[senderIdx] <= min) {
                    System.out.println("Descartando mensagem de " + m.senderIp + ":" + m.senderPort + " | Msg: " + m.msg);
                    it.remove(); 
                }
            }
        }
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
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(byteStream)) {
            
            out.writeObject(m);
            out.flush();
            byte[] buffer = byteStream.toByteArray();

            DatagramPacket packet = new DatagramPacket(
                buffer, buffer.length, InetAddress.getByName(member.ip), member.port);
            
            try (DatagramSocket socket = new DatagramSocket()) {
                socket.send(packet);
            }
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

    private static class Message implements Serializable {
        private static final long serialVersionUID = 1L;
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
    }
}