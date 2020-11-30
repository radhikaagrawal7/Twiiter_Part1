#if INTERACTIVE
#r @"bin\MCD\Debug\netcoreapp3.1\Akka.FSharp.dll"
#r @"bin\MCD\Debug\netcoreapp3.1\Akka.dll"
#r @"bin\MCD\Debug\netcoreapp3.1\FSharp.Core.dll"
//#r @"bin\MCD\Debug\netcoreapp3.1\System.Security.Cryptography.Algorithms.dll"
//#r @"bin\MCD\Debug\netcoreapp3.1\System.Text.Encoding.dll"
#endif
#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"

open System
open Akka.FSharp
open Akka.Actor
open System.Collections.Generic
open System.Collections.Concurrent
open FSharp.Collections

let mutable stopWatch = System.Diagnostics.Stopwatch()

let mutable tweet_feed = ConcurrentDictionary<string,List<string>>()
let mutable hashtags = ConcurrentDictionary<string,List<string>>()
let mutable mentions = ConcurrentDictionary<string,List<string>>()
let mutable global_sub_table = ConcurrentDictionary<string,List<string>>()



//printfn "Enter no, of users:"
let num_user = 5//(System.Console.ReadLine())|>int;
let tmp_lst = [1 .. num_user]

let listofhashtags = ["#florida";"#UF";"#cise";"#DOS";"Gainesville"]
let listofusers = List.map (fun x -> "Actor" + string(x)) tmp_lst
printfn "hash list:%A" listofhashtags

type ActorMsg =
    | TweetSave of string*string
    | Addsub of string*string*List<string>
    | Terminate

//enum for Sub Actor message
type ActorMsg2 =
    | SetName of string
    | TweetRcv of List<string>
    | Createsub of int

type ActorMsg3 =
    | Post_Tweet

let system = ActorSystem.Create("MainActor")
let mutable Actor=[]




// let config1 =
//     Configuration.parse
//         @"akka {
//             actor.provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
//             remote.helios.tcp {
//                 hostname = localhost
//                 port = 9001
//             }
//         }"
 
// let config2 =
//     Configuration.parse
//         @"akka {
//             actor.provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
//             remote.helios.tcp {
//                 hostname = localhost
//                 port = 7701
//             }
//         }" 
// let system1 = System.create "twitterClient" config1
// let system2 = System.create "twitterServer" config2

// let apiServer = system2.ActorSelection("akka.tcp://twitterServer@localhost:9001/user/server/api")







let mutable onlineusers:string list = []
let mutable offlineusers:string list = []

let mutable flag_exit = true

let mutable onlineusers_set = Set.empty<string>
let mutable offlineusers_set = Set.empty<string>

// onlineusers_set <- onlineusers_set.Add("abc")
// onlineusers_set <- onlineusers_set.Add("123")
// printfn "%A" onlineusers_set
// onlineusers_set <- onlineusers_set.Remove("123")
// let m= onlineusers_set.Contains("abc")
let mutable n_index = num_user-1


let sub_actor system name=
        let my_id = "Actor"+name
        let my_user_id = "User"+name
        let mutable myname = ""
        let mutable state = "online"
        let mutable subscriberslist = List.empty
        let mutable hashtaglist = List.empty
       
        let random = System.Random()      

        for i=0 to 1 do
            let mutable x2 = random.Next(listofhashtags.Length)
            let mutable tmp2 = listofhashtags.[x2]
            while((List.exists (fun elem -> elem = tmp2) hashtaglist) = true) do
                x2 <- random.Next(listofhashtags.Length)
                tmp2 <- listofhashtags.[x2]
            hashtaglist <- List.append hashtaglist [tmp2]

        printfn "created:%s" my_id

        let user_process = spawn system my_id <|fun mailbox ->
                            let rec loop()=
                                actor{
                                    let! message = mailbox.Receive()
                                    match message with
                                    |Createsub nmod ->
                                           for i=0 to int((n_index/nmod)) do //n_index = num_user-1
                                                let mutable x1 = random.Next(num_user)
                                                let mutable tmp = "Actor"+string((x1+1))
                                                while(((List.exists (fun elem -> elem = tmp) subscriberslist) = true) || (tmp = my_id)) do
                                                    x1 <- random.Next(num_user)
                                                    tmp <- "Actor"+string(x1+1)
                                                subscriberslist <- List.append subscriberslist [tmp]
                                           printfn "%A" subscriberslist
                                           let sel = system.ActorSelection("akka://MainActor/user/M_Actor")
                                           sel.Tell(Addsub (my_id,my_user_id,subscriberslist));
                                           

                                    |SetName nm1 ->    
                                           myname <- nm1                       
                                    |TweetRcv val2 ->
                                           printfn "received tweets:%A" val2

                                    return! loop()
                                }
                            loop()

        spawn system my_user_id <|fun mailbox ->
                            let rec loop()=
                                actor{
                                    let! message = mailbox.Receive()
                                    match message with
                                    |Post_Tweet -> 
                                        let mutable i=0
                                        let mutable j=0;
                                        for j = 0 to 1 do
                                            i <- 1
                                            while i<2 do  
                                                    i <- i+1
                                                //if state.Equals("online") then
                                                    let mutable rtrv_twt = []
                                                    for idx1 = 0 to (subscriberslist.Length-1) do
                                                        if tweet_feed.ContainsKey(subscriberslist.[idx1]) then
                                                            rtrv_twt <- tweet_feed.[subscriberslist.[idx1]]
                                                            
                                                    for idx2 = 0 to (hashtaglist.Length-1) do
                                                        if hashtags.ContainsKey(hashtaglist.[idx2]) then
                                                            let hshtg_twt = hashtags.[hashtaglist.[idx2]]
                                                            rtrv_twt <- List.append rtrv_twt hshtg_twt
                                                            //printfn ""
                                                    if mentions.ContainsKey(my_id) then
                                                        rtrv_twt <- List.append rtrv_twt mentions.[my_id]
                                                        //printfn ""

                                                    let sel_act_x = system.ActorSelection("akka://MainActor/user/M_Actor/"+my_id)
                                                    sel_act_x.Tell(TweetRcv rtrv_twt)

                                                    let h1 = random.Next(listofhashtags.Length)
                                                    let hsh = listofhashtags.[h1]
                                                    let men1 = random.Next(listofusers.Length)
                                                    let mention = listofusers.[men1]
                                                    let mk_twt = "Hey " + mention + " " + hsh  
                                                    printfn "%s" mk_twt

                                                    //below code is to add tweets corresponding to each hashtag
                                                    let check1 = hashtags.ContainsKey(hsh)
                                                    if check1 = true then
                                                        let old_list = hashtags.[hsh]
                                                        let new_list = List.append old_list [hsh]
                                                        hashtags.[hsh] <- new_list
                                                        printfn ""
                                                    else
                                                        while hashtags.TryAdd(hsh,[mk_twt]) = false do
                                                            printf ""

                                                    //below code is to add tweets corresponding to each mention
                                                    let check2 = mentions.ContainsKey(mention)
                                                    if check2 = true then
                                                        let old_list = mentions.[mention]
                                                        let new_list = List.append old_list [mention]
                                                        mentions.[mention] <- new_list
                                                        printfn ""
                                                    else
                                                        while mentions.TryAdd(mention,[mk_twt]) = false do
                                                            printf ""

                                                    let sel_act = system.ActorSelection("akka://MainActor/user/M_Actor")
                                                    sel_act.Tell(TweetSave (my_id,mk_twt))//post directly to main and let users read from main
                                                    System.Threading.Thread.Sleep(200); //change this to diff time corresponding to length of subscriberlist
                                            
                                            //code for retweet here
                                            for k = 0 to 1 do
                                                let r1 = random.Next(subscriberslist.Length)
                                                let choose_sub = subscriberslist.[r1]
                                                if tweet_feed.ContainsKey(choose_sub) then
                                                    let retweet_list = tweet_feed.[choose_sub]
                                                    let twt_idx = random.Next(retweet_list.Length)
                                                    let r_twt = retweet_list.[twt_idx]
                                                    let sel_act = system.ActorSelection("akka://MainActor/user/M_Actor")
                                                    sel_act.Tell(TweetSave (my_id,r_twt))
                                                    System.Threading.Thread.Sleep(100);

                                            //go offline                                            
                                            System.Threading.Thread.Sleep(200); //stay offline for 2sec
                                        let selact = system.ActorSelection("akka://MainActor/user/M_Actor")
                                        selact.Tell(Terminate)

                                    return! loop() 
                                }
                            loop()

            // let sel_Actor = system.ActorSelection(("akka://MainActor/user/M_Actor/User"+(1|>string)))
            // sel_Actor.Tell(Post_Tweet)

let Master_Actor num_of_node= spawn system "M_Actor" <| fun mailbox -> //Main Actor created
        Actor <-
            [1..num_of_node]
            |> List.map(fun id-> sub_actor mailbox ((string(id)))) 

        printfn "in main"       
        let mutable n_mod = 1
        for j = 1 to num_user do
            let sel_Actor = system.ActorSelection(("akka://MainActor/user/M_Actor/" + "Actor"+(j|>string)))
            sel_Actor.Tell(Createsub n_mod)
            n_mod <- n_mod+1
        
        stopWatch <- System.Diagnostics.Stopwatch.StartNew()
        

        let mutable term = 0
        printfn "Twitter server started............"
        let rec loop()=
            actor{
                let! message = mailbox.Receive()
                match message with
                |TweetSave (origin,str_twt)->
                    let check = tweet_feed.ContainsKey(origin)
                    if check = true then
                        let old_list = tweet_feed.[origin]
                        let new_list = List.append old_list [str_twt]
                        tweet_feed.[origin] <- new_list
                        printfn ""
                    else
                        while tweet_feed.TryAdd(origin,[str_twt]) = false do
                            printf ""
                |Addsub (u_name,u_id,sub_lst) ->
                    while global_sub_table.TryAdd(u_name,sub_lst) = false do
                        printf ""
                    let sel_Actor = system.ActorSelection(("akka://MainActor/user/M_Actor/"+string(u_id)))
                    sel_Actor.Tell(Post_Tweet)
                |Terminate ->
                    term <- term+1
                    printfn "term count: %d" term
                    if term = num_user then
                        flag_exit <- false
                return! loop()
            }
        loop()
 

Master_Actor num_user |>ignore

// for i=0 to num_user do
//     printfn "Enter Username:"
//     let str = System.Console.ReadLine();
//     let sel_Actor = system.ActorSelection(("akka://MainActor/user/M_Actor/" + "Actor"+(i|>string)))
//     sel_Actor.Tell(SetName str)
    //M_Actor.Tell(AddUser ((i|>string), str));

// for j=1 to num_user do
//     let sel_Actor = system.ActorSelection(("akka://MainActor/user/M_Actor/" + "User"+(j|>string)))
//     sel_Actor.Tell(Post_Tweet)

while(flag_exit) do
    printf ""

printfn "convergence time:%f" stopWatch.Elapsed.TotalMilliseconds