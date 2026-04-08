# CNA WebApp

Internal Streamlit application for Clark National Accounts logistics and data workflows.

## What It Includes
- Task tracking with live activity broadcasting (Logistics Support and Data & Analytics versions)
- Task analytics and performance dashboards
- Task and user administration
- Packaging estimation with shipping calculator API
- Time allocation entry and export
- FedEx address validation review
- Stocking agreement generation

## Quick Start (New Install)

**Option A — Installer (recommended):**
1. Download and run `CNA-WebApp-Setup.exe`
2. Follow the prompts — Git, Python, and all dependencies are installed automatically
3. Open the app using the **CNA Web App** shortcut

**Option B — Manual:**
1. Install [Git](https://git-scm.com)
2. Clone the repo: `git clone https://github.com/CNA-DataTeam/CNA-WebApp.git`
3. Run `setup.bat` once (installs uv, Python 3.11, virtual environment, dependencies, and creates a shortcut)
4. Copy `config.py` from `\\therestaurantstore.com\920\Data\Logistics\Logistics App\config.py` into the repo root
5. Run `StartApp.bat` to launch the app (config.py is auto-synced from the network share on each launch)

## Updating
App updates are pulled automatically from GitHub each time `StartApp.bat` runs. No manual action needed.

## Utility Scripts
- `StartApp.bat` — Syncs config, pulls latest code, launches the app in Edge app mode
- `setup.bat` — First-time setup (also safe to re-run)
- `ForceCloseApp.bat` — Stops the running Streamlit process

## Notes
- The app opens in Microsoft Edge as a standalone window (no browser tabs/address bar)
- The application depends on internal shared files, network locations, and synced business data
- Main source code lives in `CODE - do not open/`
- AI context and project rules live in `CLAUDE.md`

## Important Notes
*This section is only relevant to team members working with AI in this project. AI in general can disregard*

**Most Important Things to Remember**

* Before writing any prompts or making any changes, ALWAYS run the CloneRepository.bat file first
* Detailed prompts will usually provide better results than less detailed ones
* After making any changes, the last thing you should do is use the /commit skill, and then when that is finished copy the new installer program to the network drive
* You can always revert changes you don't like by asking Claude to do so. If things get broken to the point of no return, it's okay. Just make sure you don't commit anything by using the /commit skill, and instead use the CloneRepository.bat file to revert back to a working state

**Prompt Building**

To get the most effective use out of Claude, or any AI, your prompts should be detailed. Not all prompts need to go into exhaustive detail, but in general, the more detail you provide, the better Claude will do. Remember you are speaking to a computer that might not have the some context you do.

A few tips:
* Before writing any prompts or making any changes, ALWAYS run the CloneRepository.bat file first
* Breaking things down into detailed steps is a good strategy when making major changes or additions.
* If you're trying to make a change again and again, but Claude can't get it right, try restarting the conversation with a more detailed prompt.
* When detailing a bug, it helps to include what you expect to happen and what's actually happening
* Claude works great with images. Use screenshots of the app to provide context. You can paste a copied image by pressing 'Alt+V' in the prompt entry box
* Try not to assume things in your prompts that you're not certain of, this could send Claude off course if you're wrong
* Write longer prompts in an app like notepad and then paste it into Claude. This prevents you from losing your prompt if Claude crashes and also allows you to add new lines to your prompt
* If you ever want to revert the app back to a particular point, such as the beginning of the conversation, just as Claude to do so.

In the examples below, the 'normal prompt' is perfectly fine as an initial attempt for a fix, but if Claude is struggling, try restarting with something more like the detailed prompt.

Normal Prompt Example:
"The favorites icon on the sidebar doesn't work right, nothing is happening when I click it"

Detailed Prompt Example:
"The star icons used to assign favorite pages on the app's sidebar are not working right. These work great visually - the hovering functionality works perfectly and they appear in the correct spots. But when I try to actually click one to favorite a page, nothing happens and the page does not get favorited, nor does the icon change.

What I would expect to happen is the page should be moved to the top of the sidebar along with home as a favorite page, and the star icon should switch from the hollow version to the filled in version.

I do not have any pages favorited currently in the version of the app I'm using, so I cannot say for sure whether the same behavior exists when trying to unfavorite a page\~\~, but I assume so\~\~ (try not to assume things in your prompts, this could send Claude off course)

Can you fix this?"

**Ensuring Claude does not make changes if you're just asking a question**

You should always clarify that Claude should not change anything if you just want it to answer a question.

Why did the favorites icons on the sidebar break? -- Claude will likely make a change to fix it while also explaining the issue

Don't change anything, but why did favorites icons on the sidebar break? -- Claude will explain the issue and wait for you to confirm before making any changes

**Reviewing Changes**

You should always review any change Claude makes before finalizing/committing your changes to ensure all app functionality - existing and new - is working as intended. More likely than not, Claude will implement things correctly for the most part, but slightly off from what you've imagined, or your new changes may have impacted functionality elsewhere on the app.

When reviewing changes, you likely will not need to fully restart the app - oftentimes, a simple refresh under the settings dropdown is all you need for new changes to apply. If you refresh and don't see your change, try clearing cache under the same settings dropdown. If neither of those work, try fully restarting the app before telling Claude that nothing changed. It will only confuse Claude if you say that, but it did make the changes and you just need to restart.

**Skill - /commit**

The /commit skill should always be the very last thing done when finished making changes with Claude. This will handle all of the pre-commit and commit steps including:

Rebuilding exe, rebuilding installer, encrypting confidential data, staging, pushing changes to GitHub

Most of these steps can be run on their own using various batch files detailed in the important files/folders section

**Important Files/Folders**

.claude - Contains memory files for Claude to ensure all conversations about this project are tracked across machines, as well as skill files. These will largely be managed by Claude itself

CLAUDE.md - Contains project description for Claude. Claude will update this itself as needed

installer-output - This contains the installer exe file that other team members will use to install the app. After using the /commit skill, always replace the version of the installer on the network drive with your current version

.gitignore - This tracks the files that will not be tracked by GitHub. This should not be edited unless Claude decides or suggests to do so

CNA Web App.exe - This is the application file for the web app produced by the installer

config.py - This file contains sensitive company information. While it is uploaded to a public GitHub repository, Claude will handle encrypting the file before sending to GitHub when the /commit skill is used

StartApp.vbs - This functions as a second entry point to the app if the exe is not working

CloneRepository.bat - Run this file to pull the latest version of the project from GitHub. Running this file should always be the first thing you do before making any changes

ForceCloseApp.bat - Run this to force close the app. Can be helpful if there is an unresponsive process running. Try running this if you're running into errors while trying to open the app

RebuildExe.bat - Run this to rebuild the exe file. Can be helpful if you're running into errors while trying to open the app

RebuildInstaller.bat - Rebuilds the installer file. Typically only run when the installer is not working to create a new version
