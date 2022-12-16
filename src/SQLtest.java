import java.io.File;
import java.io.IOException;
import java.util.*;

class Clerk{
    public int num=0;
    private int productCount = 0;
    private Queue<String> query = new LinkedList<String>(); //query队列，里面存放所有可执行的query

    //加入SQL
    public synchronized void produceProduct() {
        String path = "/home/za/File/Git_File/Fintech-Benchmark/adapter/mysql/query";
        File directory = new File(path);
        File[] queryFiles = directory.listFiles();
        ArrayList<String> querys = new ArrayList<String>();

        // 读取目录下所有sql文件，将每条sql保存到querys中
        for (int i = 0; i < queryFiles.length; i++) {
            try{
                Scanner scanner = new Scanner(queryFiles[i].toPath());
                String query = "";
                while(scanner.hasNextLine()){
                    query = query + " " + scanner.nextLine();
                }
                querys.add(query);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if(productCount < 20){
            //todo:把这里的sql换成从文件中读

            // 随机选取一个查询加入队列
            int queryNum = new Random().nextInt(querys.size() - 1);
            this.query.offer(querys.get(queryNum));
//            System.out.println(querys.get(queryNum));
            productCount++;
            //System.out.println(Thread.currentThread().getName() + ": 加入第" + productCount + "条查询");
            notify();
        }else{
            //等待
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    //执行查询
    public synchronized String consumeProduct() {
        String query="";
        if(productCount > 0){
//            System.out.println(Thread.currentThread().getName() + ":开始执行第" + productCount + "条查询");
            productCount--;
            query=this.query.poll();
            notify();
        }else{
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return query;
    }

}

class Producer extends Thread{//生产者
    private Clerk clerk;

    public Producer(Clerk clerk){
        this.clerk = clerk;
    }

    @Override
    public void run() {
        while(!Thread.currentThread().isInterrupted()){
            clerk.produceProduct();
        }
    }
}

class Consumer extends Thread{  //消费者
    private Clerk clerk;
    private Client client;

    public Consumer(Clerk clerk){
        this.clerk = clerk;
        //todo:这里要改成对应的配置信息
        this.client=new Client("127.0.0.1","testdb","3306","root","daslab3008");
    }

    @Override
    public void run() {
        while(!Thread.currentThread().isInterrupted()){
            String query=clerk.consumeProduct();
            boolean result=this.client.execSQL(query);
            //System.out.println("result is:"+result);
            if (result==true){
                this.clerk.num++;
            }
        }
    }

    public void kill_client(){
        this.client.release();
    }
}

public class SQLtest {
    public static void main(String[] args) {

        int interval= 5000; //执行测试的时间,单位ms

        Clerk clerk = new Clerk();
        Producer p1 = new Producer(clerk);
        p1.setName("producer 1");
//        Producer p2 = new Producer(clerk);
//        p2.setName("producer 2");
//        Producer p3 = new Producer(clerk);
//        p3.setName("producer 3");

        long start=System.currentTimeMillis(); //start time in ms
        p1.start();

        ArrayList<Consumer> workers=new ArrayList<Consumer>();
        int i;
        for(i=0;i<40;i++){  // rec: tidb-50 mysql-40
            Consumer cs = new Consumer(clerk);
            cs.setName("client"+i);
            workers.add(cs);
            cs.start();
        }
        System.out.println("started "+i+" clients");

        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                for (Consumer c:
                     workers) {
                    c.interrupt();
                    c.kill_client();
                }
                p1.stop();

                double time_passed= ((System.currentTimeMillis()-start)/1000.0);
                double qps = clerk.num / time_passed;

                System.out.println("total tasks: "+clerk.num+"\nqps:"+qps);
            }
        },interval);
    }
}
