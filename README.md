# small_tools
一些python，shell脚本，也可能有简单的java工程，主要用于数据处理，大数据集群等服务维护的有用小程序，经过生产实践检验。
yarn_surv.py :新增基于yarn restful API的超时任务扫描然然后杀掉占用资源过多的任务的小程序(scan applications based on yarn restful api, and kill the apps which are overtime and occupy too much resource)