This WebCrawler program was done in Python 3.8 using the packages urllib, sqlite3 and BeautifulSoup. 

It is an update of my original version and includes some additional functionalities and tweaks. 
These include the following, many of which are marked as updates in the comment sections of the code along with what they do and why they were placed there.

Updates:
DocumentDictionary: fixed table to match that of my proprietary search engine.
Inserted the Porter Stemming algorithm for token processing.
Included a count for detected stop words.
A dedicated method norm_url() was added for normalising url names, and overhauling the crawlcomplete while loop for URL navigation, queuing, and soup integration for text extraction. 
Early try statements updated changing the process from its original last-in first-out (LIFO) to first-in first-out (FIFO) for how it navigates pages.  Now opts for a breadth first search instead of a depth first search. 
if links_queue < 500 statement updated with how it processes and handles URLs before adding them to the queue.
Links retrieval format updated to better handle different page types and protocols, such as http, https, and protocol relative URLs that start with //.
Updated to skip over links that were clearly marked as JavaScript files, emails (mailto:), and fragment-only links (generally unpredictable, broken or generated links, such as URL based requests for forms or submissions).
Added an else at the end for skipping over any rarer cases of link fragmentation that made it past previous checks.
Added a check at the end as an extra layer to avoid duplicating queued URLs by also checking the tocrawl list alongside the crawled list.
Updated the BeautifulSoup usage in the crawlcomplete loop and the try statement for retrieving URL pages

Note: 
This program first navigates all the pages closest from the search before navigating further in. 
Previously, it would search down one path and if it was deep enough, the crawler wouldn’t be able to reach other pages within the main site.
The BeautifulSoup is used for the variables soup, paragraphs, and the main tok variable. It is what extracts the text from each page to be indexed. 
It only extracts text that appears within <p> tags as the pages main content, ignoring all other html, script and styling code around the text through the .find_all() method. 
Then when it is given to the tok variable, all styling and other text formatting code is filtered out using the .get_text() method for each found paragraph section. 
Both methods are part of the BeautifulSoup package. The try statement further above it was updated to handle page redirects through the requests package as well as simpler crawler detection.
This was done by setting a user type for the URL retrieval and a timeout. This also made it easier to handle each page which was previously being output into byte code.

Disclaimer:
This web crawler is for educational reasons only and exists as a WIP framework. 
It does not include robot.txt handling or timed events on actions and should not be used on public websites.
