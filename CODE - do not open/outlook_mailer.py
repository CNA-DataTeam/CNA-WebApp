"""
outlook_mailer.py

Minimal, headless Outlook COM sender for unattended app jobs (e.g. the scheduled
time-allocation reminder). Sends mail PROGRAMMATICALLY via classic desktop
Outlook on the machine running the job — there is no SMTP/service-account path.

"Send as" a shared mailbox (the load-bearing detail):
    To send AS a shared mailbox (e.g. CNAConsole), the mailbox must be ADDED TO
    CLASSIC OUTLOOK as an additional mailbox so it mounts as a Store. We then read
    that store's owner as a real Exchange (type "EX") directory object and set the
    message's "sent representing" identity from it. With Send-As permission Exchange
    accepts this and the message goes out cleanly AS the mailbox.

    Setting only SentOnBehalfOfName to the raw SMTP string does NOT work: Outlook
    turns it into an unresolved SMTP "on behalf of" entry, so Exchange runs the
    Send-ON-BEHALF permission check (not Send-As) and rejects it with
    MapiExceptionSendAsDenied. The mounted-store EX identity is what the manual
    From-dropdown uses under the hood. If the mailbox is NOT mounted we fall back
    to SentOnBehalfOfName and log a warning (that send will bounce without
    Send-on-Behalf rights).

Requires classic desktop Outlook (the "New Outlook"/web client exposes no COM,
and a shared mailbox added only to New Outlook is invisible here — it must be in
classic). pywin32 (win32com) + pythoncom must be importable. Raises on failure so
callers can log and continue.
"""

from __future__ import annotations

import logging

LOGGER = logging.getLogger("outlook_mailer")

_OL_MAIL_ITEM = 0  # win32 Outlook OlItemType.olMailItem

# MAPI proptags
_PR_MAILBOX_OWNER_ENTRYID = "http://schemas.microsoft.com/mapi/proptag/0x661B0102"
_PR_SENT_REPRESENTING_ENTRYID = "http://schemas.microsoft.com/mapi/proptag/0x00410102"
_PR_SENT_REPRESENTING_NAME_W = "http://schemas.microsoft.com/mapi/proptag/0x0042001F"
_PR_SENT_REPRESENTING_ADDRTYPE_W = "http://schemas.microsoft.com/mapi/proptag/0x0064001F"
_PR_SENT_REPRESENTING_EMAIL_ADDRESS_W = "http://schemas.microsoft.com/mapi/proptag/0x0065001F"


def _owner_address_entry_for_smtp(ns, smtp_address: str):
    """Return the Exchange (EX) AddressEntry of a mounted store whose mailbox owner
    has the given primary SMTP address, or None if no such store is mounted."""
    target = (smtp_address or "").strip().lower()
    if not target:
        return None
    try:
        stores = ns.Stores
    except Exception:
        return None
    for index in range(1, int(getattr(stores, "Count", 0)) + 1):
        try:
            store = stores.Item(index)
            owner_bin = store.PropertyAccessor.GetProperty(_PR_MAILBOX_OWNER_ENTRYID)
            owner_hex = store.PropertyAccessor.BinaryToString(owner_bin)
            ae = ns.GetAddressEntryFromID(owner_hex)
        except Exception:
            continue
        if ae is None or str(getattr(ae, "Type", "")).upper() != "EX":
            continue
        primary = ""
        try:
            exu = ae.GetExchangeUser()
            if exu is not None:
                primary = str(exu.PrimarySmtpAddress or "")
        except Exception:
            primary = ""
        if primary.lower() == target:
            return ae
    return None


def send_html_mail(
    to: str,
    subject: str,
    html_body: str,
    from_address: str | None = None,
    cc: str | None = None,
) -> str:
    """Send one HTML email via Outlook. Returns how the From was set (for logging).

    Raises if Outlook is unavailable or the send fails.
    """
    if not str(to or "").strip():
        raise ValueError("send_html_mail: 'to' is required")

    import pythoncom
    import win32com.client as win32

    pythoncom.CoInitialize()
    try:
        outlook = win32.Dispatch("Outlook.Application")
        ns = outlook.GetNamespace("MAPI")
        mail = outlook.CreateItem(_OL_MAIL_ITEM)
        mail.To = to
        if cc:
            mail.CC = cc
        mail.Subject = subject
        mail.HTMLBody = html_body

        from_mode = "default-profile"
        from_address = (from_address or "").strip()
        if from_address:
            owner = _owner_address_entry_for_smtp(ns, from_address)
            if owner is not None:
                pa = mail.PropertyAccessor
                pa.SetProperty(_PR_SENT_REPRESENTING_ENTRYID, pa.StringToBinary(owner.ID))
                pa.SetProperty(_PR_SENT_REPRESENTING_NAME_W, owner.Name)
                pa.SetProperty(_PR_SENT_REPRESENTING_ADDRTYPE_W, "EX")
                pa.SetProperty(_PR_SENT_REPRESENTING_EMAIL_ADDRESS_W, owner.Address)
                from_mode = f"send-as(EX):{from_address}"
            else:
                # Mailbox not mounted in classic Outlook — best effort; will bounce
                # without Send-on-Behalf rights. Surface this loudly.
                LOGGER.warning(
                    "Shared mailbox '%s' is not mounted in classic Outlook; falling back to "
                    "SentOnBehalfOfName (this will bounce unless Send-on-Behalf is granted). "
                    "Add the mailbox to classic Outlook to send as it.",
                    from_address,
                )
                mail.SentOnBehalfOfName = from_address
                from_mode = f"on-behalf(smtp):{from_address}"

        mail.Send()
        return from_mode
    finally:
        pythoncom.CoUninitialize()
