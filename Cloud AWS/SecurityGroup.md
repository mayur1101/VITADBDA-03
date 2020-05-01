**Security groups**
- A security group acts as a virtual firewall that controls the traffic for one or more instances.

- When you launch an instance, you can specify one or more security groups; otherwise, AWS use the default security group.

- You can add rules to each security group that allow traffic to or from its associated instances.

- You can modify the rules for a security group at any time; the new rules are automatically applied to all instances that are associated with the security group.

- When we decide whether to allow traffic to reach an instance, we evaluate all the rules from all the security groups that are associated with the instance.

- When you create a security group, it has no inbound rules. Therefore, no inbound traffic originating from another host to your instance is allowed until you add inbound rules to the security group.

- By default, a security group includes an outbound rule that allows all outbound traffic. You can remove the rule and add outbound rules that allow specific outbound traffic only. If your security group has no outbound rules, no outbound traffic originating from your instance is allowed.

- Security groups are stateful � if you send a request from your instance, the response traffic for that request is allowed to flow in regardless of inbound security group rules. Responses to allowed inbound traffic are allowed to flow out, regardless of outbound rules.
 

   [link](https://docs.aws.amazon.com/vpc/latest/userguide/VPC_SecurityGroups.html)