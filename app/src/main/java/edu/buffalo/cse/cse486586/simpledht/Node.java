package edu.buffalo.cse.cse486586.simpledht;

import android.util.Log;

/**
 * Created by charanya on 3/29/18.
 */

public class Node implements Comparable<Node>{

        public String port_num;
        public String hash_port;
        public Node succesor = null;
        public Node predecessor = null;

        public Node(String port_num,String hash_port){
            this.port_num = port_num;
            this.hash_port = hash_port;
        }

        public int compareTo(Node node)
        {
            if((this.hash_port.compareTo(node.hash_port)) > 1)
                return 1;
            else if((this.hash_port.compareTo(node.hash_port)) < 1)
                return -1;
            else
                return 0;
        }
        public void setSuccesor(Node node){
            this.succesor = node;
        }
        public void setPredecessor(Node node){
            this.predecessor = node;
        }

/*
        @Override
        public String toString(){
            return "[Port = "+port_num+",hashed value ="+hash_port+",succ.node = "+succesor+",pred.node ="+predecessor+"]";
        }
*/
}
