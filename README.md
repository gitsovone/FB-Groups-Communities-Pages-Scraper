**FB Groups and Pages scraper**

**Description**

`	`***main.py*** 

`	`***main.sh*** to check if *main.py* works, if not, it starts.

***config.py*** contains information for connecting to the database, you should create own config file

***models.py*** contains a description of the models for the worker, resource, publication and post attachments (pictures, videos and external links)

***regex.py*** contains regular expressions to search for links, publication date, text, attachments and more. internal elements.

***new\_async\_parser.py*** main parser logic

***pyppeteer\_parser.py*** parsing through the browser, there is a link to *new\_async\_parser.py*. Not completed.

The Facebook scraping is based on asynchronous requests through a proxy.

In *main.py* in the main function, the initial reading of the worker configuration (**status=enabled**) from the database occurs, and the creation of separate processes for each defined worker. In an endless loop, configuration updates are checked and the worker is running. If the worker terminates, a new process is created with the changed configuration parameters, provided that status is still enabled.

In **create\_parser\_worker**(*main.py*) an instance of the **FBParser**(*new\_async\_parser.py*) class is created, passing the worker configuration and calling the method defined in **type* *.

The **FBParser** class, when initialized, takes the parameters passed by the worker (proxy, parsing depth, etc.) and creates various attributes necessary for work.

The main working methods (completed) are:

- ***parse\_groups*** - scraping groups and communities.
- ***find\_groups*** - definition of groups from the general list of unsorted resources.
- ***find\_communities*** - identifying communities from a common list of unsorted resources.
- ***clean\_communities*** - methods used to identify communities additionally identifies resources suitable for parsing without an account through a browser. This method separates such resources. It is advisable to run several times after *find\_communities*.

` `Requests are executed in batches, the size of the batch is selected depending on the capabilities of the proxy. On current mobile proxies, the ratio of requests / stability is 200 requests. The parameter is controlled by the **max\_requests** property (defined in the code, consider moving it to the worker settings). Also, the need to move the timeout settings and the number of requests to the worker parameters before changing the ip address

**Methods for defining groups and communities:**

There is a request for data on this link, the groups have a common scheme for generating links of the form:

`	`***https://m.facebook.com/groups/fb\_resource\_id***
**
` `*fb\_resource\_id* can be in text format

` `If the link matches the given pattern, an attempt is made to fetch the data. Upon successful extraction of data, the resource is defined as a group, and the corresponding mark **type=1** is added to the local database in fb\_parser.resource\_social.

` `The following request to the facebook internal API is used to define communities:

[***https://m.facebook.com/page_content_list_view/more/?page_id={fb_resource_id}&start_cursor=7&num_to_fetch=10&surface_type=time](https://m.facebook.com/page_content_list_view/more/?page_id=%7Bfb_resource_id%7D&start_cursor=7&num_to_fetch=10&surface_type=timeline)***line***

An attempt is made to extract information from the query results. If successful, the resource is assigned **type=2**. Using this link, you can get community data and, I don’t know how this category is called correctly, but you can get a mobile version of it only using accounts. This query for this category does not return the last 4 posts, you may need to experiment with the query parameters, but I did not get to that.

` `For this query, you can change the number of publications that we want to get by changing the parameter *num\_to\_fetch.*

` `The community cleanup method requests a resource by reference:

`	`***https://m.facebook.com/fb\_resource\_id***
\***
` `If redirecting from mobile to home, then **type=3**.

` `Facebook does not always return a uniform number of posts per page, for groups the value can be from 0 to 8. For communities, it almost always matches the query parameter. Posts that are video posts are not returned (they are added to the page with JavaScript and therefore are not included in the request request). The same thing happens with the first four posts from type=3, they query with GraphQL, and I was not able to quickly fake the query (may be possible with a more detailed study of the principle of forming parameters for these queries). So far, parsing of such resources is possible using a browser or using accounts.

**parse\_groups:**

` `First of all, the id of publications that have already been added to the local database are loaded in accordance with the parsing depth. It is necessary to exclude parsing of the same publications.

``The cycle runs in two stages. Groups are queried first from the database, then communities. Resources owned by the current worker, marked for parsing (stability), corresponding to the current period of activity (activity) and having less than 500 parsing errors in a row (errors) are requested.

` `Next, a list of resources is formed in accordance with the Resource(models.py) model and passed in batches (max\_requests) to aiohttp(*\_load\_pages, \_fetch\_page*) requests, the requests are asynchronous, which allows for stable operation proxy to process a pack in 5-6 seconds. In case of problems with the proxy, requests are interrupted by a timeout.

` `The requested data is passed to the *\_find\_posts* method, which determines the posts to be added to the system. An appropriate list of publications is created (containing id, uri, publish\_time) according to the Post(models.py) model. When the accumulation of posts is sufficient to form a batch of requests, the pages of publications are loaded and the text and attachments are determined in the *\_parse\_posts* method. Publications are written to the local database, on successful parsing status=1, on failure status=0. Attachments receive a status depending on the type of content: 1 for images, 2 for videos, and 3 for external links. And so on until all the resources for parsing the current worker run out.

**\_find\_posts**

` `Method to find the id and time of the publication.

` `It receives a Resource object as input, in the *page\_html* field of which the loaded page is stored. Facebook stores post data in blocks, we are interested in top\_level\_post\_id and publish\_time fields. For convenience, the page is split by the top\_level\_post\_id field and looped through all the elements with the regular expression *post\_finder\_regex*(regex.py). The resulting parameters are checked for compliance with the parsing depth and the absence of already processed publications in the list. Based on the publications that passed the check, objects are created according to the Post model and added to the list of publications of the resource. This method also checks for the success of parsing and the activity of the resource.

**\_parse\_posts**

` `A method for extracting the text and attachments of a publication.

` `It takes a list of Resource or Post objects as input. Further, all publications are sorted out in a cycle, for each publication, a page for parsing is extracted from the *page\_html* field. In the general case, two blocks were identified in the HTML structure containing the information of interest.

` `It often happens that there are several posts and other information on the page that can be included in the final result. The *\_splice\_post\_content* method cuts out the necessary block, cutting off unnecessary content. The resulting block is divided into two parts, with the content of the publication and with attachments. The text is extracted from the content, excluding the title (contains text like “User shared a post in a group” or other variations). Attachments can contain various information, including text. Based on observations (about 4000 pages viewed, yes I'm a fool), the following information extraction algorithm was developed:

- the text of attachments is searched;
- searching for photos. If found, the presence of the main message text is checked. If the main text is missing, the attachment text is assigned to it and the attachment text is deleted;
- searching for links to external resources. In case of detection, they are assigned the text of attachments (remember the actions in the previous paragraph);
- searching for links to video files. In case of detection, they are assigned the text of attachments (remember the actions on the text).

It was not possible to detect the simultaneous fulfillment of all conditions, such a structure takes into account all detected page variants.

All methods use regular expressions to extract the required information, except for *\_splice\_post\_content*.

**\_parse\_attachments**

To get a link to a photo/image in maximum resolution, you need to make an additional transition, which is implemented here.

**reparse\_posts, reparse\_attachments**

During the parsing process, there are times when facebook does not allow you to get the page (proxy or something). In this case, publications and attachments that are already defined but have incomplete information are written to the local database with a certain status. For publications - status=0, for attachments type=11. These methods in the current implementation of the parser are executed after the main loop and retry to get the information.

**\_write\_to\_db**

Writes the results of parsing to the local database.

For resources, changes are made regarding the number of publications, source activity and parsing errors.

For publications - parsing status, resource affiliation, identifier, text, publication date, publication link and parsing errors. Posts whose content has been removed or restricted receive status=0 and errors=10 to be excluded from *reparse\_posts*.

For attachments - belonging to the resource and publication, link, text (if any), attachment type and errors.

**\_update\_posts, \_update\_attachments**

`	`Methods for updating data when *reparse\_posts, reparse\_attachments* methods work